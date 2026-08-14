/**
 * server.js
 * Servidor WebSocket (biblioteca "ws") responsável por sincronizar em tempo
 * real a posição, direção e avatar de todos os jogadores dentro de cada
 * cenário (sala) da Feira Virtual — substitui o polling do MVP inicial.
 *
 * Fluxo:
 *  1. O cliente (frontend) chama backend/api/gerar_ws_token.php logado via
 *     sessão PHP, recebendo um token assinado (HMAC) + a URL deste servidor.
 *  2. O cliente abre uma conexão WebSocket e manda { type: "join", token, cenario_id, tileX, tileY, avatarConfig }.
 *  3. Este servidor valida a assinatura do token (mesmo segredo do PHP),
 *     coloca o jogador numa INSTÂNCIA daquele cenário (ver "Instâncias" abaixo) e:
 *       - responde ao próprio cliente com "room_state" (todos que já estão na instância)
 *       - avisa os demais da instância com "player_joined"
 *  4. A cada movimento, o cliente manda { type: "move", tileX, tileY, direcao },
 *     que é retransmitido aos outros como "player_moved" (sem tocar no banco),
 *     com um limite de frequência (ver "Throttle" abaixo).
 *  5. Ao desconectar (fechar aba, cair a conexão), avisamos a instância com "player_left".
 *
 * --- Instâncias (controle de lotação) ---
 * Cada cenário (ex: "Praça Central", cenario_id=1) pode ter várias INSTÂNCIAS
 * simultâneas — cópias independentes da mesma sala, cada uma com seu próprio
 * grupo de jogadores. Isso evita que uma única sala WebSocket acumule gente
 * demais (o custo de rede de retransmitir "fulano se moveu" cresce com o
 * QUADRADO do número de jogadores na mesma sala, não com o total de lojas
 * cadastradas). Quando uma instância atinge LIMITE_JOGADORES_POR_INSTANCIA,
 * o próximo jogador a entrar é automaticamente colocado numa instância nova
 * (ou numa existente com vaga) — o mesmo conceito usado pelo Gather.town
 * quando um espaço "enche". O cliente nem precisa saber disso: ele só manda
 * cenario_id, o servidor decide a instância internamente.
 *
 * --- Throttle (limite de frequência do broadcast de movimento) ---
 * O jogo já limita naturalmente a cadência de movimento no cliente (cada
 * passo só é enviado quando o tile anterior termina de ser percorrido), mas
 * este servidor também aplica um teto defensivo de ~10 broadcasts/segundo
 * por jogador — protege contra clientes modificados, velocidades futuras
 * mais rápidas, ou picos de mensagens, sem prejudicar a fluidez visual (o
 * cliente sempre interpola suavemente até a última posição recebida).
 *
 * Rodar: copie .env.example para .env, ajuste o segredo (igual ao do PHP),
 * depois `npm install && npm start`.
 */

require('dotenv').config();
const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

// O Render (e a maioria dos PaaS) atribui a porta dinamicamente via a variável
// de ambiente PORT — o servidor PRECISA escutar nela em produção. Localmente,
// sem essa variável definida, cai no WS_PORT do .env (padrão 8080).
const WS_PORT = Number(process.env.PORT || process.env.WS_PORT || 8080);
const WS_SHARED_SECRET = process.env.WS_SHARED_SECRET || 'troque-este-segredo-em-producao-32chars';

// Quantos jogadores cabem em UMA instância de um cenário antes do servidor
// começar a abrir uma cópia nova. Ajustável por variável de ambiente sem
// precisar mexer no código — em produção, comece testando com algo entre
// 30 e 60 e ajuste conforme observar o uso real de CPU/rede do serviço.
const LIMITE_JOGADORES_POR_INSTANCIA = Number(process.env.LIMITE_JOGADORES_POR_INSTANCIA || 40);

// Intervalo mínimo (ms) entre dois broadcasts de "player_moved" do MESMO
// jogador. 100ms ≈ 10 broadcasts/segundo no máximo por pessoa.
const INTERVALO_MIN_BROADCAST_MOVE_MS = Number(process.env.INTERVALO_MIN_BROADCAST_MOVE_MS || 100);

/**
 * salasPorCenario: Map<cenarioId, Map<instanciaId, Map<usuarioId, jogadorState>>>
 * jogadorState = { ws, usuarioId, nome, tileX, tileY, direcao, avatarConfig, _ultimoBroadcastMove }
 */
const salasPorCenario = new Map();

/** Acha a instância em que um usuário JÁ está dentro de um cenário (ex: deu F5 na aba), se houver. */
function encontrarInstanciaDoUsuario(cenarioId, usuarioId) {
  const instancias = salasPorCenario.get(cenarioId);
  if (!instancias) return null;
  for (const [instanciaId, sala] of instancias) {
    if (sala.has(usuarioId)) return { instanciaId, sala };
  }
  return null;
}

/** Acha uma instância com vaga (ou cria uma nova) para um jogador recém-chegado. */
function obterInstanciaComVaga(cenarioId) {
  if (!salasPorCenario.has(cenarioId)) salasPorCenario.set(cenarioId, new Map());
  const instancias = salasPorCenario.get(cenarioId);

  for (const [instanciaId, sala] of instancias) {
    if (sala.size < LIMITE_JOGADORES_POR_INSTANCIA) return { instanciaId, sala };
  }

  // Todas as instâncias existentes estão cheias (ou não existe nenhuma ainda): abre uma nova cópia da sala.
  const novaInstanciaId = instancias.size + 1;
  const novaSala = new Map();
  instancias.set(novaInstanciaId, novaSala);
  console.log(`[realtime-server] Cenário ${cenarioId} lotado — abrindo instância #${novaInstanciaId}.`);
  return { instanciaId: novaInstanciaId, sala: novaSala };
}

function removerInstanciaSeVazia(cenarioId, instanciaId) {
  const instancias = salasPorCenario.get(cenarioId);
  if (!instancias) return;
  const sala = instancias.get(instanciaId);
  if (sala && sala.size === 0) {
    instancias.delete(instanciaId);
    if (instancias.size === 0) salasPorCenario.delete(cenarioId);
  }
}

/** Decodifica base64url (mesmo esquema usado no PHP: +/ -> -_, sem padding). */
function base64UrlDecode(str) {
  const base64 = str.replace(/-/g, '+').replace(/_/g, '/') + '='.repeat((4 - (str.length % 4)) % 4);
  return Buffer.from(base64, 'base64').toString('utf8');
}

/**
 * Valida o token gerado por backend/api/gerar_ws_token.php.
 * Formato: base64url(payload_json) + "." + hmac_sha256(payload_b64, SEGREDO)
 * Retorna o payload ({usuario_id, nome, exp}) se válido, ou null caso contrário.
 */
function verificarToken(token) {
  if (typeof token !== 'string' || !token.includes('.')) return null;

  const [payloadB64, assinaturaRecebida] = token.split('.');
  const assinaturaEsperada = crypto.createHmac('sha256', WS_SHARED_SECRET).update(payloadB64).digest('hex');

  // Comparação em tempo constante para evitar timing attacks
  const bufRecebida = Buffer.from(assinaturaRecebida || '', 'hex');
  const bufEsperada = Buffer.from(assinaturaEsperada, 'hex');
  if (bufRecebida.length !== bufEsperada.length || !crypto.timingSafeEqual(bufRecebida, bufEsperada)) {
    return null;
  }

  try {
    const payload = JSON.parse(base64UrlDecode(payloadB64));
    if (!payload.usuario_id || !payload.exp || payload.exp < Math.floor(Date.now() / 1000)) {
      return null; // token expirado ou incompleto
    }
    return payload;
  } catch {
    return null;
  }
}

function enviar(ws, tipo, dados) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify({ type: tipo, ...dados }));
  }
}

/** Envia uma mensagem para todos os jogadores de UMA instância, exceto (opcionalmente) um usuarioId. */
function difundirParaInstancia(cenarioId, instanciaId, tipo, dados, ignorarUsuarioId = null) {
  const sala = salasPorCenario.get(cenarioId)?.get(instanciaId);
  if (!sala) return;
  for (const [usuarioId, jogador] of sala.entries()) {
    if (usuarioId === ignorarUsuarioId) continue;
    enviar(jogador.ws, tipo, dados);
  }
}

// Cria um servidor HTTP "de verdade" (em vez de deixar a lib "ws" criar um
// implícito) por dois motivos:
//   1) Diagnóstico: dá pra abrir https://<seu-servico>.onrender.com/ no
//      navegador e ver uma resposta 200 confirmando que o serviço está de pé
//      — sem isso, um GET normal nessa URL fica pendurado sem resposta, o que
//      também pode derrubar health checks HTTP configurados manualmente no Render.
//   2) É o padrão recomendado pela própria documentação do Render para apps
//      WebSocket em Node: https://render.com/docs/websocket
const servidorHttp = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Feira Virtual — servidor de tempo real (WebSocket) está no ar.');
});

const wss = new WebSocketServer({ server: servidorHttp });

servidorHttp.listen(WS_PORT, () => {
  console.log(`[realtime-server] Servidor WebSocket da Feira Virtual escutando na porta ${WS_PORT}`);
  console.log(`[realtime-server] Limite por instância: ${LIMITE_JOGADORES_POR_INSTANCIA} jogadores | Throttle de movimento: ${INTERVALO_MIN_BROADCAST_MOVE_MS}ms`);
});

wss.on('connection', (ws) => {
  // Estado da conexão preenchido no "join"
  ws.contexto = { usuarioId: null, cenarioId: null, instanciaId: null, vivo: true };

  ws.on('pong', () => { ws.contexto.vivo = true; });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return enviar(ws, 'erro', { mensagem: 'Mensagem inválida (JSON malformado).' });
    }

    switch (msg.type) {
      case 'join': {
        const payload = verificarToken(msg.token);
        if (!payload) {
          enviar(ws, 'erro', { mensagem: 'Token inválido ou expirado. Faça login novamente.' });
          return ws.close();
        }

        const cenarioId = Number(msg.cenario_id) || 1;
        const usuarioId = Number(payload.usuario_id);

        // Se o usuário já está em alguma instância deste cenário (ex: deu F5
        // na aba), reaproveita a MESMA instância em vez de sortear uma nova —
        // assim ele continua vendo as mesmas pessoas de antes do reload.
        let { instanciaId, sala } = encontrarInstanciaDoUsuario(cenarioId, usuarioId) || {};
        if (sala) {
          const antiga = sala.get(usuarioId).ws;
          if (antiga !== ws && antiga.readyState === antiga.OPEN) antiga.close();
        } else {
          ({ instanciaId, sala } = obterInstanciaComVaga(cenarioId));
        }

        ws.contexto.usuarioId = usuarioId;
        ws.contexto.cenarioId = cenarioId;
        ws.contexto.instanciaId = instanciaId;

        const estadoJogador = {
          ws,
          usuarioId,
          nome: payload.nome,
          tileX: Number(msg.tileX) || 0,
          tileY: Number(msg.tileY) || 0,
          direcao: msg.direcao || 'baixo',
          avatarConfig: msg.avatarConfig || {},
          _ultimoBroadcastMove: 0,
        };
        sala.set(usuarioId, estadoJogador);

        // 1) Envia ao recém-chegado o snapshot atual da instância (todos os outros jogadores dela)
        const outros = [...sala.values()]
          .filter((j) => j.usuarioId !== usuarioId)
          .map((j) => ({
            usuario_id: j.usuarioId, nome: j.nome, tileX: j.tileX, tileY: j.tileY,
            direcao: j.direcao, avatarConfig: j.avatarConfig,
          }));
        enviar(ws, 'room_state', { jogadores: outros });

        // 2) Avisa os demais da instância que alguém entrou
        difundirParaInstancia(cenarioId, instanciaId, 'player_joined', {
          usuario_id: usuarioId, nome: payload.nome,
          tileX: estadoJogador.tileX, tileY: estadoJogador.tileY,
          direcao: estadoJogador.direcao, avatarConfig: estadoJogador.avatarConfig,
        }, usuarioId);

        console.log(`[realtime-server] ${payload.nome} (#${usuarioId}) entrou no cenário ${cenarioId} (instância #${instanciaId}). Jogadores nessa instância: ${sala.size}`);
        break;
      }

      case 'move': {
        const { usuarioId, cenarioId, instanciaId } = ws.contexto;
        if (!usuarioId || !cenarioId) return; // ainda não fez "join"

        const sala = salasPorCenario.get(cenarioId)?.get(instanciaId);
        const jogador = sala?.get(usuarioId);
        if (!jogador) return;

        // O estado interno é sempre atualizado (importante pra quem entrar
        // depois receber a posição certa via room_state) — só o BROADCAST
        // pros outros jogadores é que respeita o teto de frequência.
        jogador.tileX = Number(msg.tileX);
        jogador.tileY = Number(msg.tileY);
        jogador.direcao = msg.direcao || jogador.direcao;

        const agora = Date.now();
        if (agora - jogador._ultimoBroadcastMove < INTERVALO_MIN_BROADCAST_MOVE_MS) {
          return; // muito cedo desde o último broadcast deste jogador — descarta silenciosamente
        }
        jogador._ultimoBroadcastMove = agora;

        difundirParaInstancia(cenarioId, instanciaId, 'player_moved', {
          usuario_id: usuarioId, tileX: jogador.tileX, tileY: jogador.tileY, direcao: jogador.direcao,
        }, usuarioId);
        break;
      }

      case 'avatar_update': {
        const { usuarioId, cenarioId, instanciaId } = ws.contexto;
        if (!usuarioId || !cenarioId) return;

        const sala = salasPorCenario.get(cenarioId)?.get(instanciaId);
        const jogador = sala?.get(usuarioId);
        if (!jogador) return;

        jogador.avatarConfig = msg.avatarConfig || jogador.avatarConfig;
        difundirParaInstancia(cenarioId, instanciaId, 'player_avatar_updated', {
          usuario_id: usuarioId, avatarConfig: jogador.avatarConfig,
        }, usuarioId);
        break;
      }

      default:
        enviar(ws, 'erro', { mensagem: `Tipo de mensagem desconhecido: ${msg.type}` });
    }
  });

  ws.on('close', () => {
    const { usuarioId, cenarioId, instanciaId } = ws.contexto;
    if (usuarioId == null || cenarioId == null || instanciaId == null) return;

    const sala = salasPorCenario.get(cenarioId)?.get(instanciaId);
    if (sala && sala.get(usuarioId)?.ws === ws) {
      sala.delete(usuarioId);
      difundirParaInstancia(cenarioId, instanciaId, 'player_left', { usuario_id: usuarioId });
      console.log(`[realtime-server] Usuário #${usuarioId} saiu do cenário ${cenarioId} (instância #${instanciaId}). Jogadores nessa instância: ${sala.size}`);
      removerInstanciaSeVazia(cenarioId, instanciaId);
    }
  });
});

// Heartbeat: fecha conexões mortas (ex: aba fechada sem handshake de close) a cada 30s.
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.contexto && ws.contexto.vivo === false) return ws.terminate();
    if (ws.contexto) ws.contexto.vivo = false;
    ws.ping();
  });
}, 30000);
