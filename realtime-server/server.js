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
 *     registra o jogador na "sala" (Map por cenario_id) e:
 *       - responde ao próprio cliente com "room_state" (todos que já estão na sala)
 *       - avisa os demais da sala com "player_joined"
 *  4. A cada movimento, o cliente manda { type: "move", tileX, tileY, direcao },
 *     que é retransmitido aos outros como "player_moved" (sem tocar no banco).
 *  5. Ao desconectar (fechar aba, cair a conexão), avisamos a sala com "player_left".
 *
 * Rodar: copie .env.example para .env, ajuste o segredo (igual ao do PHP),
 * depois `npm install && npm start`.
 */

require('dotenv').config();
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

// O Render (e a maioria dos PaaS) atribui a porta dinamicamente via a variável
// de ambiente PORT — o servidor PRECISA escutar nela em produção. Localmente,
// sem essa variável definida, cai no WS_PORT do .env (padrão 8080).
const WS_PORT = Number(process.env.PORT || process.env.WS_PORT || 8080);
const WS_SHARED_SECRET = process.env.WS_SHARED_SECRET || 'troque-este-segredo-em-producao-32chars';

/** Salas: Map<cenarioId, Map<usuarioId, { ws, nome, tileX, tileY, direcao, avatarConfig }>> */
const salas = new Map();

function getSala(cenarioId) {
  if (!salas.has(cenarioId)) salas.set(cenarioId, new Map());
  return salas.get(cenarioId);
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

/** Envia uma mensagem para todos os jogadores da sala, exceto (opcionalmente) um usuarioId. */
function difundirParaSala(cenarioId, tipo, dados, ignorarUsuarioId = null) {
  const sala = salas.get(cenarioId);
  if (!sala) return;
  for (const [usuarioId, jogador] of sala.entries()) {
    if (usuarioId === ignorarUsuarioId) continue;
    enviar(jogador.ws, tipo, dados);
  }
}

const wss = new WebSocketServer({ port: WS_PORT });
console.log(`[realtime-server] Servidor WebSocket da Feira Virtual escutando na porta ${WS_PORT}`);

wss.on('connection', (ws) => {
  // Estado da conexão preenchido no "join"
  ws.contexto = { usuarioId: null, cenarioId: null, vivo: true };

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

        ws.contexto.usuarioId = usuarioId;
        ws.contexto.cenarioId = cenarioId;

        const sala = getSala(cenarioId);

        // Se o mesmo usuário já tinha uma conexão antiga nesta sala (ex: reload da aba), substitui.
        if (sala.has(usuarioId)) {
          const antiga = sala.get(usuarioId).ws;
          if (antiga !== ws && antiga.readyState === antiga.OPEN) antiga.close();
        }

        const estadoJogador = {
          ws,
          usuarioId,
          nome: payload.nome,
          tileX: Number(msg.tileX) || 0,
          tileY: Number(msg.tileY) || 0,
          direcao: msg.direcao || 'baixo',
          avatarConfig: msg.avatarConfig || {},
        };
        sala.set(usuarioId, estadoJogador);

        // 1) Envia ao recém-chegado o snapshot atual da sala (todos os outros jogadores)
        const outros = [...sala.values()]
          .filter((j) => j.usuarioId !== usuarioId)
          .map((j) => ({
            usuario_id: j.usuarioId, nome: j.nome, tileX: j.tileX, tileY: j.tileY,
            direcao: j.direcao, avatarConfig: j.avatarConfig,
          }));
        enviar(ws, 'room_state', { jogadores: outros });

        // 2) Avisa os demais que alguém entrou
        difundirParaSala(cenarioId, 'player_joined', {
          usuario_id: usuarioId, nome: payload.nome,
          tileX: estadoJogador.tileX, tileY: estadoJogador.tileY,
          direcao: estadoJogador.direcao, avatarConfig: estadoJogador.avatarConfig,
        }, usuarioId);

        console.log(`[realtime-server] ${payload.nome} (#${usuarioId}) entrou no cenário ${cenarioId}. Jogadores na sala: ${sala.size}`);
        break;
      }

      case 'move': {
        const { usuarioId, cenarioId } = ws.contexto;
        if (!usuarioId || !cenarioId) return; // ainda não fez "join"

        const sala = getSala(cenarioId);
        const jogador = sala.get(usuarioId);
        if (!jogador) return;

        jogador.tileX = Number(msg.tileX);
        jogador.tileY = Number(msg.tileY);
        jogador.direcao = msg.direcao || jogador.direcao;

        difundirParaSala(cenarioId, 'player_moved', {
          usuario_id: usuarioId, tileX: jogador.tileX, tileY: jogador.tileY, direcao: jogador.direcao,
        }, usuarioId);
        break;
      }

      case 'avatar_update': {
        const { usuarioId, cenarioId } = ws.contexto;
        if (!usuarioId || !cenarioId) return;

        const sala = getSala(cenarioId);
        const jogador = sala.get(usuarioId);
        if (!jogador) return;

        jogador.avatarConfig = msg.avatarConfig || jogador.avatarConfig;
        difundirParaSala(cenarioId, 'player_avatar_updated', {
          usuario_id: usuarioId, avatarConfig: jogador.avatarConfig,
        }, usuarioId);
        break;
      }

      default:
        enviar(ws, 'erro', { mensagem: `Tipo de mensagem desconhecido: ${msg.type}` });
    }
  });

  ws.on('close', () => {
    const { usuarioId, cenarioId } = ws.contexto;
    if (usuarioId == null || cenarioId == null) return;

    const sala = salas.get(cenarioId);
    if (sala && sala.get(usuarioId)?.ws === ws) {
      sala.delete(usuarioId);
      difundirParaSala(cenarioId, 'player_left', { usuario_id: usuarioId });
      console.log(`[realtime-server] Usuário #${usuarioId} saiu do cenário ${cenarioId}. Jogadores na sala: ${sala.size}`);
      if (sala.size === 0) salas.delete(cenarioId);
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
