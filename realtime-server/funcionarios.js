/**
 * funcionarios.js
 * "Cérebro" dos funcionários-IA (agentes vendedores) — módulo separado do
 * server.js pra manter a responsabilidade isolada: aqui só cuida de buscar
 * o catálogo (quais funcionários existem, onde ficam as lojas, quais
 * produtos vender), simular o comportamento (patrulhar/abordar) e gerar as
 * falas. Quem chama isso e distribui via WebSocket é o server.js.
 *
 * Por que HTTP e não conexão direta ao MySQL? O InfinityFree BLOQUEIA
 * conexão externa ao banco (só aceita conexão vinda de dentro da própria
 * hospedagem) — então, em vez de duplicar credenciais de banco aqui, este
 * módulo consulta um endpoint PHP público (listar_funcionarios_ativos.php)
 * periodicamente e mantém tudo em cache local, em memória.
 */

// Node 18+ já tem fetch global — sem dependência extra.

const PHP_API_BASE_URL = (process.env.PHP_API_BASE_URL || '').replace(/\/+$/, '');
const ATUALIZAR_CATALOGO_INTERVALO_MS = Number(process.env.ATUALIZAR_CATALOGO_INTERVALO_MS || 3 * 60 * 1000); // 3 min
const VELOCIDADE_TICK_MS = Number(process.env.FUNCIONARIOS_TICK_MS || 450); // frequência do "pensamento" de cada funcionário
const COOLDOWN_ABORDAGEM_MS = [25000, 45000]; // intervalo (min,max) aleatório de espera após abordar alguém

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || null;
const ANTHROPIC_MODEL = process.env.ANTHROPIC_MODEL || 'claude-haiku-4-5-20251001'; // modelo leve/rápido, adequado a falas curtas de NPC

/** Catálogo em cache, atualizado periodicamente a partir do PHP. */
let catalogo = {
  lojasPorCenario: new Map(), // Map<cenarioId, [{id, stand_pos_x, ..., cor_loja, nome_loja, status}]>
  cenarios: new Map(),        // Map<cenarioId, {largura_grid, altura_grid}>
  funcionariosPorCenario: new Map(), // Map<cenarioId, [funcionario com dados da loja embutidos]>
  produtosPorLoja: new Map(), // Map<lojaId, [nomes]>
};

function log(...args) {
  console.log('[funcionarios-ia]', ...args);
}

async function atualizarCatalogo() {
  if (!PHP_API_BASE_URL) {
    log('PHP_API_BASE_URL não configurada — funcionários-IA desativados (defina no .env pra habilitar).');
    return;
  }
  try {
    const resp = await fetch(`${PHP_API_BASE_URL}/listar_funcionarios_ativos.php`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const dados = await resp.json();

    const lojasPorCenario = new Map();
    (dados.lojas || []).forEach((l) => {
      const lista = lojasPorCenario.get(l.cenario_id) || [];
      lista.push(l);
      lojasPorCenario.set(l.cenario_id, lista);
    });

    const cenarios = new Map();
    (dados.cenarios || []).forEach((c) => cenarios.set(c.id, c));

    const funcionariosPorCenario = new Map();
    (dados.funcionarios || []).forEach((f) => {
      const lista = funcionariosPorCenario.get(f.cenario_id) || [];
      lista.push(f);
      funcionariosPorCenario.set(f.cenario_id, lista);
    });

    const produtosPorLoja = new Map(Object.entries(dados.produtos_por_loja || {}).map(([k, v]) => [Number(k), v]));

    catalogo = { lojasPorCenario, cenarios, funcionariosPorCenario, produtosPorLoja };
    log(`Catálogo atualizado: ${dados.funcionarios?.length || 0} funcionário(s) ativo(s) em ${funcionariosPorCenario.size} cenário(s).`);
  } catch (err) {
    log('Falha ao buscar catálogo do PHP (mantendo o cache anterior):', err.message);
  }
}

/** Todos os funcionários de todos os cenários, num Map<funcionarioId, dadosCompletos> — usado pra achar os dados completos na hora de gerar uma fala. */
function encontrarFuncionarioPorId(funcionarioId) {
  for (const lista of catalogo.funcionariosPorCenario.values()) {
    const encontrado = lista.find((f) => f.id === funcionarioId);
    if (encontrado) return encontrado;
  }
  return null;
}

/** Monta o conjunto de tiles bloqueados (footprint dos stands) de um cenário — usado pro funcionário não atravessar loja alheia. */
function construirColisao(cenarioId) {
  const bloqueados = new Set();
  (catalogo.lojasPorCenario.get(cenarioId) || []).forEach((loja) => {
    for (let dy = 0; dy < loja.altura_tiles; dy++) {
      for (let dx = 0; dx < loja.largura_tiles; dx++) {
        bloqueados.add(`${loja.stand_pos_x + dx},${loja.stand_pos_y + dy}`);
      }
    }
  });
  return bloqueados;
}

function tileLivre(bloqueados, limites, x, y) {
  if (x < 0 || y < 0 || x >= limites.largura_grid || y >= limites.altura_grid) return false;
  return !bloqueados.has(`${x},${y}`);
}

function escolherAleatorio(lista) {
  return lista[Math.floor(Math.random() * lista.length)];
}

/** Substitui {produto} e {loja} numa frase-molde de saudação. */
function montarFalaTemplate(funcionario, produtos) {
  const frase = escolherAleatorio(funcionario.mensagens_saudacao) || 'Dá uma olhada aqui na loja!';
  const produto = produtos && produtos.length ? escolherAleatorio(produtos) : 'nossos produtos';
  return frase.replace(/\{produto\}/gi, produto).replace(/\{loja\}/gi, funcionario.nome_loja);
}

/**
 * Gera a fala via Anthropic API de verdade, SE ANTHROPIC_API_KEY estiver
 * configurada e o funcionário tiver personalidade_ia preenchida. Retorna
 * null em qualquer falha (sem API key, timeout, erro de rede) — quem chamou
 * cai automaticamente para montarFalaTemplate() nesse caso.
 */
async function gerarFalaComIA(funcionario, produtos) {
  if (!ANTHROPIC_API_KEY || !funcionario.personalidade_ia) return null;

  const produto = produtos && produtos.length ? escolherAleatorio(produtos) : 'os produtos da loja';
  const prompt = `Você é ${funcionario.nome}, funcionário(a) de "${funcionario.nome_loja}" numa feira virtual gamificada. Personalidade: ${funcionario.personalidade_ia}. Produto em destaque agora: "${produto}". Escreva UMA frase curta (máximo 18 palavras) abordando um visitante que acabou de passar perto do seu stand, em português do Brasil, tom de vendedor animado mas não insistente. Responda SOMENTE com a frase, sem aspas e sem explicação.`;

  try {
    const resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: ANTHROPIC_MODEL,
        max_tokens: 60,
        messages: [{ role: 'user', content: prompt }],
      }),
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const dados = await resp.json();
    const texto = dados?.content?.find((b) => b.type === 'text')?.text?.trim();
    return texto || null;
  } catch (err) {
    log(`Falha ao gerar fala via IA pra ${funcionario.nome} (usando frase padrão):`, err.message);
    return null;
  }
}

/**
 * Decide a fala de abordagem: tenta IA generativa (com timeout de segurança
 * pra não travar o NPC esperando de mais) e cai pro banco de frases se não rolar.
 */
async function obterFalaAbordagem(funcionario) {
  const produtos = catalogo.produtosPorLoja.get(funcionario.loja_id) || [];

  const falaIA = await Promise.race([
    gerarFalaComIA(funcionario, produtos),
    new Promise((resolve) => setTimeout(() => resolve(null), 4000)),
  ]);

  return falaIA || montarFalaTemplate(funcionario, produtos);
}

module.exports = {
  atualizarCatalogo,
  iniciarAtualizacaoPeriodica: () => setInterval(atualizarCatalogo, ATUALIZAR_CATALOGO_INTERVALO_MS),
  getCatalogo: () => catalogo,
  encontrarFuncionarioPorId,
  construirColisao,
  tileLivre,
  obterFalaAbordagem,
  VELOCIDADE_TICK_MS,
  COOLDOWN_ABORDAGEM_MS,
};
