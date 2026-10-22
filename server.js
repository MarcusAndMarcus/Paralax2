#!/usr/bin/env node
/*
 * PARALLAX · Células — web service de cosmologia (matéria escura e energia escura)
 * 17 células, uma por telescópio, cada uma com 8 instâncias Claude Sonnet em papéis
 * adversariais; troca entre células por gossip sobre o barramento URB1; Polo V
 * determinístico (nenhum número nasce de modelo de linguagem).
 *
 * Arquivo único, Node 20+, zero dependências. Gerado a partir de src/ por build.js.
 *
 *   node server.js            inicia o servidor (HOST=0.0.0.0, PORT do Render)
 *   node server.js --teste    autoteste offline (sem rede, sem API)
 *   node server.js --validar  baixa os dados públicos e confere os pipelines contra
 *                             as cadeias oficiais publicadas
 *   node server.js --simular  ciclo completo das 17 células em modo simulado
 *   node server.js --simular-api  ciclo contra um servidor falso da Messages API
 */
'use strict';

const http = require('node:http');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');

const VERSAO = '1.0.0';

/* ── 01 · configuração ─────────────────────────────────────────────────── */
const CFG = (() => {
  const e = process.env;
  const num = (v, d) => (v === undefined || v === '' || !Number.isFinite(Number(v))) ? d : Number(v);
  const chave = e.ANTHROPIC_API_KEY || '';
  return {
    PORT: num(e.PORT, 3000),
    HOST: e.HOST || '0.0.0.0',
    ANTHROPIC_API_KEY: chave,
    ANTHROPIC_URL: e.ANTHROPIC_URL || 'https://api.anthropic.com/v1/messages',
    ANTHROPIC_VERSION: '2023-06-01',
    MODELO: e.MODELO || 'claude-sonnet-5-5',
    // output_config.effort: low | medium | high | xhigh | max
    ESFORCO: e.ESFORCO || 'medium',
    ESFORCO_CRITICO: e.ESFORCO_CRITICO || 'high',
    MAX_TOKENS: num(e.MAX_TOKENS, 16000),
    CONCORRENCIA: num(e.CONCORRENCIA, 6),
    RPM: num(e.RPM, 40),
    TETO_USD: num(e.TETO_USD, 150),
    // preços do Claude Sonnet 5.5 (US$ por milhão de tokens), conferidos em
    // platform.claude.com/docs/en/models/overview em 16/out/2026
    PRECO_ENTRADA: num(e.PRECO_ENTRADA_USD_MTOK, 2),
    PRECO_SAIDA: num(e.PRECO_SAIDA_USD_MTOK, 10),
    // cache de prompt (platform.claude.com/docs/en/build-with-claude/prompt-caching, 20/out/2026):
    // escrita de 5 min = 1,25× a entrada; leitura no Sonnet 5.5 = 0,05× a entrada
    FATOR_CACHE_ESCRITA: num(e.FATOR_CACHE_ESCRITA, 1.25),
    FATOR_CACHE_LEITURA: num(e.FATOR_CACHE_LEITURA, 0.05),
    CACHE_PROMPT: e.CACHE_PROMPT !== '0',
    ACCESS_TOKEN: e.ACCESS_TOKEN || '',
    DATA_DIR: e.DATA_DIR || path.join(process.cwd(), 'dados'),
    MODO: e.MODO || (chave ? 'real' : 'simulado'),
    RODADAS_MAX: num(e.RODADAS_MAX, 3),
    LIMIAR_TENSAO: num(e.LIMIAR_TENSAO, 3),
    LIMIAR_PESO: num(e.LIMIAR_PESO, 0.5),
    PASSOS_FERRAMENTA: num(e.PASSOS_FERRAMENTA, 8),
    TIMEOUT_API_MS: num(e.TIMEOUT_API_MS, 600000),
    TIMEOUT_DADOS_MS: num(e.TIMEOUT_DADOS_MS, 180000),
    MCMC_PASSOS: num(e.MCMC_PASSOS, 20000),
    PRODUCAO: e.NODE_ENV === 'production' || !!e.RENDER,
    SIMULAR_ALUCINACAO: e.SIMULAR_ALUCINACAO || '',
  };
})();

/* ── 02 · protocolo URB1 ───────────────────────────────────────────────────
 * Mesmo quadro do UROBOROS:
 *   magic 55524231 ("URB1") | ver(1) | tipo(1) | len(2 BE) | payload | crc16(2 BE)
 * CRC16-CCITT, polinômio 0x1021, init 0xFFFF, sem reflexão, sem XOR final.
 */
const URB1 = (() => {
  const MAGIC = 0x55524231;
  const TABELA = (() => {
    const t = new Uint16Array(256);
    for (let i = 0; i < 256; i++) {
      let c = i << 8;
      for (let b = 0; b < 8; b++) c = (c & 0x8000) ? ((c << 1) ^ 0x1021) & 0xffff : (c << 1) & 0xffff;
      t[i] = c;
    }
    return t;
  })();
  function crc16(bytes) {
    let crc = 0xffff;
    for (const b of bytes) crc = ((crc << 8) ^ TABELA[((crc >> 8) ^ b) & 0xff]) & 0xffff;
    return crc;
  }
  const TIPO = { EVENTO: 0x01, TENSOR: 0x03, VEREDITO: 0x04, MEDIDA: 0x05, TENSAO: 0x06, GOSSIP: 0x07 };
  const NOME_TIPO = Object.fromEntries(Object.entries(TIPO).map(([k, v]) => [v, k]));
  function enquadrar(tipo, payloadUtf8, versao = 1) {
    const corpo = Buffer.from(payloadUtf8, 'utf8');
    if (corpo.length > 0xffff) throw new Error('URB1: payload excede 65535 bytes');
    const cab = Buffer.alloc(8);
    cab.writeUInt32BE(MAGIC, 0);
    cab.writeUInt8(versao, 4);
    cab.writeUInt8(tipo, 5);
    cab.writeUInt16BE(corpo.length, 6);
    const semCrc = Buffer.concat([cab, corpo]);
    const crc = Buffer.alloc(2);
    crc.writeUInt16BE(crc16(semCrc), 0);
    return Buffer.concat([semCrc, crc]).toString('hex').toUpperCase();
  }
  function desenquadrar(hex) {
    if (typeof hex !== 'string' || !/^[0-9a-fA-F]*$/.test(hex) || hex.length % 2) throw new Error('URB1: hex inválido');
    const buf = Buffer.from(hex, 'hex');
    if (buf.length < 10) throw new Error('URB1: quadro curto');
    if (buf.readUInt32BE(0) !== MAGIC) throw new Error('URB1: magic inválido');
    const len = buf.readUInt16BE(6);
    if (buf.length !== 8 + len + 2) throw new Error('URB1: comprimento divergente');
    const esperado = buf.readUInt16BE(8 + len);
    const obtido = crc16(buf.subarray(0, 8 + len));
    if (esperado !== obtido) throw new Error(`URB1: CRC divergente ${obtido.toString(16)}!=${esperado.toString(16)}`);
    return { versao: buf.readUInt8(4), tipo: buf.readUInt8(5), payload: buf.subarray(8, 8 + len).toString('utf8') };
  }
  return { MAGIC, TIPO, NOME_TIPO, crc16, enquadrar, desenquadrar };
})();

/* ── 03 · utilidades ───────────────────────────────────────────────────── */
const U = (() => {
  const sha256 = (dados) => crypto.createHash('sha256').update(dados).digest('hex');
  let contador = 0;
  function novoId(prefixo, semente) {
    contador++;
    const h = sha256(String(semente || '') + '|' + Date.now() + '|' + contador + '|' + Math.random()).slice(0, 8);
    return prefixo + '-' + h;
  }
  const dormir = (ms) => new Promise((r) => setTimeout(r, ms));

  /* Limitador de concorrência + requisições por minuto (janela deslizante). */
  function criarLimitador(maxConc, rpm) {
    let ativos = 0;
    const espera = [];
    const inicios = [];
    let timer = null;
    function tentar() {
      const agora = Date.now();
      while (inicios.length && agora - inicios[0] >= 60000) inicios.shift();
      while (espera.length && ativos < maxConc && (!rpm || inicios.length < rpm)) {
        ativos++;
        inicios.push(Date.now());
        espera.shift()();
      }
      if (espera.length && ativos < maxConc && rpm && inicios.length >= rpm && !timer) {
        const falta = Math.max(25, 60000 - (agora - inicios[0]) + 5);
        timer = setTimeout(() => { timer = null; tentar(); }, falta);
      }
    }
    async function limitar(fn) {
      await new Promise((r) => { espera.push(r); tentar(); });
      try { return await fn(); } finally { ativos--; tentar(); }
    }
    limitar.estado = () => ({ ativos, fila: espera.length, ultimoMinuto: inicios.length });
    return limitar;
  }

  /* Formatação numérica pt-BR com casas fixas (vírgula decimal). */
  function fmt(x, casas = 3) {
    if (x === null || x === undefined || !Number.isFinite(x)) return '—';
    return x.toFixed(casas).replace('.', ',').replace('-', '−');
  }
  function casasPara(sigma) {
    if (!Number.isFinite(sigma) || sigma <= 0) return 4;
    const d = Math.ceil(-Math.log10(sigma)) + 1;
    return Math.min(8, Math.max(0, d));
  }
  function jaccard(a, b) {
    const A = new Set((a || []).map(normalizarAssercao)), B = new Set((b || []).map(normalizarAssercao));
    if (!A.size && !B.size) return 1;
    let inter = 0;
    for (const x of A) if (B.has(x)) inter++;
    return inter / (A.size + B.size - inter);
  }
  function normalizarAssercao(s) {
    return String(s).toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '').replace(/\s+/g, ' ').trim();
  }
  function json(x, limite) {
    let s;
    try { s = JSON.stringify(x); } catch (e) { s = JSON.stringify({ erro: 'não serializável: ' + e.message }); }
    if (limite && s.length > limite) s = s.slice(0, limite) + '…[truncado em ' + limite + ' caracteres]';
    return s;
  }
  function seguroHTML(s) {
    return String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }
  function iguaisSeguro(a, b) {
    const ha = crypto.createHash('sha256').update(String(a)).digest();
    const hb = crypto.createHash('sha256').update(String(b)).digest();
    return crypto.timingSafeEqual(ha, hb);
  }
  /* Gerador pseudoaleatório determinístico (mulberry32) para MCMC reprodutível. */
  function rng(semente) {
    let a = semente >>> 0;
    return function () {
      a |= 0; a = (a + 0x6D2B79F5) | 0;
      let t = Math.imul(a ^ (a >>> 15), 1 | a);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }
  function gauss(r) {
    let u = 0, v = 0;
    while (u === 0) u = r();
    while (v === 0) v = r();
    return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
  }
  /* Mensagem de erro de rede com a causa (o fetch do Node diz só "fetch failed"). */
  function causa(e) {
    const c = e && e.cause;
    const det = c ? (c.code || c.message || String(c)) : '';
    return String(e && e.message || e) + (det && !String(e && e.message).includes(det) ? ' (' + det + ')' : '');
  }
  return { sha256, novoId, dormir, criarLimitador, fmt, casasPara, jaccard, normalizarAssercao, json, seguroHTML, iguaisSeguro, rng, gauss, causa };
})();

/* ── 04 · álgebra, otimização e estatística (determinístico) ─────────────── */
const ALG = (() => {
  /* Cholesky de matriz simétrica positiva definida n×n (row-major). Devolve L. */
  function cholesky(A, n) {
    const L = new Float64Array(n * n);
    for (let j = 0; j < n; j++) {
      const Lj = j * n;
      let s = A[Lj + j];
      for (let k = 0; k < j; k++) s -= L[Lj + k] * L[Lj + k];
      if (!(s > 0)) throw new Error('matriz não é positiva definida (pivô ' + j + ', valor ' + s + ')');
      const d = Math.sqrt(s);
      L[Lj + j] = d;
      for (let i = j + 1; i < n; i++) {
        const Li = i * n;
        let t = A[Li + j];
        for (let k = 0; k < j; k++) t -= L[Li + k] * L[Lj + k];
        L[Li + j] = t / d;
      }
    }
    return L;
  }
  /* Resolve L y = b (L triangular inferior). */
  function resolverInferior(L, n, b) {
    const y = new Float64Array(n);
    for (let i = 0; i < n; i++) {
      const Li = i * n;
      let s = b[i];
      for (let k = 0; k < i; k++) s -= L[Li + k] * y[k];
      y[i] = s / L[Li + i];
    }
    return y;
  }
  /* Resolve Lᵀ x = y. */
  function resolverSuperiorT(L, n, y) {
    const x = new Float64Array(n);
    for (let i = n - 1; i >= 0; i--) {
      let s = y[i];
      for (let k = i + 1; k < n; k++) s -= L[k * n + i] * x[k];
      x[i] = s / L[i * n + i];
    }
    return x;
  }
  function logDetCholesky(L, n) { let s = 0; for (let i = 0; i < n; i++) s += Math.log(L[i * n + i]); return 2 * s; }
  function matVec(W, n, x) {
    const y = new Float64Array(n);
    for (let i = 0; i < n; i++) { const Wi = i * n; let s = 0; for (let k = 0; k < n; k++) s += W[Wi + k] * x[k]; y[i] = s; }
    return y;
  }
  function dot(a, b) { let s = 0; for (let i = 0; i < a.length; i++) s += a[i] * b[i]; return s; }

  /* Inversa de matriz pequena (array de arrays) por Gauss-Jordan com pivô parcial. */
  function inversaPequena(M) {
    const n = M.length;
    const A = M.map((r, i) => [...r, ...Array.from({ length: n }, (_, j) => (i === j ? 1 : 0))]);
    for (let c = 0; c < n; c++) {
      let p = c;
      for (let r = c + 1; r < n; r++) if (Math.abs(A[r][c]) > Math.abs(A[p][c])) p = r;
      if (Math.abs(A[p][c]) < 1e-300) throw new Error('matriz singular');
      [A[c], A[p]] = [A[p], A[c]];
      const piv = A[c][c];
      for (let k = 0; k < 2 * n; k++) A[c][k] /= piv;
      for (let r = 0; r < n; r++) if (r !== c) { const f = A[r][c]; if (f) for (let k = 0; k < 2 * n; k++) A[r][k] -= f * A[c][k]; }
    }
    return A.map((r) => r.slice(n));
  }
  function choleskyPequena(M) {
    const n = M.length, A = new Float64Array(n * n);
    for (let i = 0; i < n; i++) for (let j = 0; j < n; j++) A[i * n + j] = M[i][j];
    const L = cholesky(A, n);
    return Array.from({ length: n }, (_, i) => Array.from({ length: n }, (_, j) => L[i * n + j]));
  }

  /* Nelder–Mead. f devolve Infinity fora do domínio. */
  function nelderMead(f, x0, passos, opcoes = {}) {
    const d = x0.length, maxIter = opcoes.maxIter || 4000, tol = opcoes.tol || 0, tolAbs = opcoes.tolAbs ?? 1e-8;
    let simplex = [x0.slice()];
    for (let i = 0; i < d; i++) { const v = x0.slice(); v[i] += passos[i]; simplex.push(v); }
    let valores = simplex.map(f);
    let iter = 0;
    for (; iter < maxIter; iter++) {
      const ordem = valores.map((v, i) => i).sort((a, b) => valores[a] - valores[b]);
      simplex = ordem.map((i) => simplex[i]); valores = ordem.map((i) => valores[i]);
      if (Math.abs(valores[d] - valores[0]) <= tolAbs + tol * Math.abs(valores[0]) && iter > 20) break;
      const c = new Array(d).fill(0);
      for (let i = 0; i < d; i++) for (let k = 0; k < d; k++) c[k] += simplex[i][k] / d;
      const xr = c.map((ck, k) => ck + (ck - simplex[d][k]));
      const fr = f(xr);
      if (fr < valores[0]) {
        const xe = c.map((ck, k) => ck + 2 * (ck - simplex[d][k]));
        const fe = f(xe);
        if (fe < fr) { simplex[d] = xe; valores[d] = fe; } else { simplex[d] = xr; valores[d] = fr; }
      } else if (fr < valores[d - 1]) {
        simplex[d] = xr; valores[d] = fr;
      } else {
        const fora = fr < valores[d];
        const xc = fora ? c.map((ck, k) => ck + 0.5 * (xr[k] - ck)) : c.map((ck, k) => ck + 0.5 * (simplex[d][k] - ck));
        const fc = f(xc);
        if (fc < (fora ? fr : valores[d])) { simplex[d] = xc; valores[d] = fc; }
        else {
          for (let i = 1; i <= d; i++) { simplex[i] = simplex[i].map((v, k) => simplex[0][k] + 0.5 * (v - simplex[0][k])); valores[i] = f(simplex[i]); }
        }
      }
    }
    const ordem = valores.map((v, i) => i).sort((a, b) => valores[a] - valores[b]);
    return { x: simplex[ordem[0]], f: valores[ordem[0]], iter };
  }
  /* Mínimo com reinícios (Nelder–Mead tende a parar cedo em vales estreitos). */
  function minimizar(f, x0, passos, opcoes = {}) {
    let r = nelderMead(f, x0, passos, opcoes);
    for (let k = 0; k < (opcoes.reinicios || 3); k++) {
      const r2 = nelderMead(f, r.x, passos.map((p) => p / (2 * (k + 1))), opcoes);
      if (r2.f < r.f - 1e-12) r = r2; else if (Math.abs(r2.f - r.f) < 1e-12) break;
    }
    return r;
  }

  /* Hessiana numérica de f (χ²) por diferenças centrais. */
  function hessiana(f, x, h) {
    const d = x.length, H = Array.from({ length: d }, () => new Array(d).fill(0));
    const f0 = f(x);
    const em = (deltas) => f(x.map((v, i) => v + (deltas[i] || 0)));
    for (let i = 0; i < d; i++) {
      const di = new Array(d).fill(0); di[i] = h[i];
      const dm = new Array(d).fill(0); dm[i] = -h[i];
      H[i][i] = (em(di) - 2 * f0 + em(dm)) / (h[i] * h[i]);
      for (let j = 0; j < i; j++) {
        const pp = new Array(d).fill(0), pm = new Array(d).fill(0), mp = new Array(d).fill(0), mm = new Array(d).fill(0);
        pp[i] = h[i]; pp[j] = h[j]; pm[i] = h[i]; pm[j] = -h[j]; mp[i] = -h[i]; mp[j] = h[j]; mm[i] = -h[i]; mm[j] = -h[j];
        H[i][j] = H[j][i] = (em(pp) - em(pm) - em(mp) + em(mm)) / (4 * h[i] * h[j]);
      }
    }
    return H;
  }
  /* Covariância de Laplace: −ln L = χ²/2 ⇒ Cov = 2·H(χ²)⁻¹. Duas passadas de passo. */
  function covarianciaLaplace(f, x, escalas) {
    let h = escalas.slice(), cov = null;
    for (let passada = 0; passada < 2; passada++) {
      const H = hessiana(f, x, h);
      cov = inversaPequena(H).map((r) => r.map((v) => 2 * v));
      const sig = cov.map((r, i) => Math.sqrt(Math.max(r[i], 0)));
      if (sig.some((s) => !Number.isFinite(s) || s <= 0)) throw new Error('Hessiana não positiva no mínimo');
      h = sig.map((s) => 0.5 * s);
    }
    return cov;
  }

  /* Metropolis com proposta gaussiana a partir da covariância de Laplace.
     Assíncrono: cede o laço de eventos a cada 200 passos (SSE segue vivo). */
  async function mcmc(logPost, x0, cov, opcoes = {}) {
    const d = x0.length, passos = opcoes.passos || 20000, aquec = opcoes.aquecimento || Math.floor(passos / 5);
    const r = U.rng(opcoes.semente || 20261016);
    let escala = 2.38 / Math.sqrt(d);
    let Lc = choleskyPequena(cov);
    let x = x0.slice(), lp = logPost(x);
    if (!Number.isFinite(lp)) throw new Error('ponto inicial fora do suporte do posterior');
    const amostras = [];
    let aceitos = 0, aceitosAquec = 0;
    for (let t = 0; t < passos + aquec; t++) {
      if (t % 200 === 199) await new Promise((res) => setImmediate(res));
      if (opcoes.sinal && opcoes.sinal.aborted) throw new Error('ciclo interrompido');
      const z = Array.from({ length: d }, () => U.gauss(r));
      const prop = x.map((v, i) => { let s = 0; for (let k = 0; k <= i; k++) s += Lc[i][k] * z[k]; return v + escala * s; });
      const lpp = logPost(prop);
      if (Number.isFinite(lpp) && Math.log(r()) < lpp - lp) { x = prop; lp = lpp; if (t >= aquec) aceitos++; else aceitosAquec++; }
      if (t === Math.floor(aquec / 2) && aquec > 200) {
        const taxa = aceitosAquec / (aquec / 2);
        if (taxa < 0.15) escala *= 0.6; else if (taxa > 0.45) escala *= 1.5;
      }
      if (t >= aquec) amostras.push(x.slice());
    }
    const resumo = [];
    for (let i = 0; i < d; i++) {
      const col = amostras.map((a) => a[i]).sort((a, b) => a - b);
      const m = col.reduce((s, v) => s + v, 0) / col.length;
      const sd = Math.sqrt(col.reduce((s, v) => s + (v - m) * (v - m), 0) / (col.length - 1));
      const q = (p) => col[Math.min(col.length - 1, Math.max(0, Math.floor(p * (col.length - 1))))];
      resumo.push({ media: m, desvio: sd, mediana: q(0.5), p16: q(0.15865), p84: q(0.84135), p2: q(0.02275), p97: q(0.97725) });
    }
    return { resumo, aceitacao: aceitos / passos, n: amostras.length };
  }

  /* Funções especiais para p-valores. */
  function lnGamma(x) {
    const g = 7, c = [0.99999999999980993, 676.5203681218851, -1259.1392167224028, 771.32342877765313,
      -176.61502916214059, 12.507343278686905, -0.13857109526572012, 9.9843695780195716e-6, 1.5056327351493116e-7];
    if (x < 0.5) return Math.log(Math.PI / Math.sin(Math.PI * x)) - lnGamma(1 - x);
    x -= 1;
    let a = c[0];
    const t = x + g + 0.5;
    for (let i = 1; i < g + 2; i++) a += c[i] / (x + i);
    return 0.5 * Math.log(2 * Math.PI) + (x + 0.5) * Math.log(t) - t + Math.log(a);
  }
  /* Q(a,x) = Γ(a,x)/Γ(a), gama incompleta regularizada superior. */
  function gamaQ(a, x) {
    if (x < 0 || a <= 0) return NaN;
    if (x === 0) return 1;
    if (x < a + 1) {
      let ap = a, soma = 1 / a, del = soma;
      for (let n = 0; n < 1000; n++) { ap++; del *= x / ap; soma += del; if (Math.abs(del) < Math.abs(soma) * 1e-15) break; }
      return 1 - soma * Math.exp(-x + a * Math.log(x) - lnGamma(a));
    }
    let b = x + 1 - a, c = 1 / 1e-300, d = 1 / b, h = d;
    for (let i = 1; i < 1000; i++) {
      const an = -i * (i - a); b += 2;
      d = an * d + b; if (Math.abs(d) < 1e-300) d = 1e-300;
      c = b + an / c; if (Math.abs(c) < 1e-300) c = 1e-300;
      d = 1 / d; const del = d * c; h *= del;
      if (Math.abs(del - 1) < 1e-15) break;
    }
    return Math.exp(-x + a * Math.log(x) - lnGamma(a)) * h;
  }
  const pQuiQuadrado = (chi2, k) => gamaQ(k / 2, chi2 / 2);
  function erfc(x) {
    // erfc(x) = Q(1/2, x²) para x ≥ 0
    return x >= 0 ? gamaQ(0.5, x * x) : 2 - gamaQ(0.5, x * x);
  }
  /* Converte p-valor bilateral em número de σ gaussianos equivalentes. */
  function sigmaDeP(p) {
    if (!(p > 0)) return Infinity;
    if (p >= 1) return 0;
    let lo = 0, hi = 40;
    for (let i = 0; i < 200; i++) { const m = (lo + hi) / 2; if (erfc(m / Math.SQRT2) > p) lo = m; else hi = m; }
    return (lo + hi) / 2;
  }

  /* Estatística descritiva de uma coluna numérica. */
  function descrever(valores) {
    const v = valores.filter((x) => typeof x === 'number' && Number.isFinite(x)).sort((a, b) => a - b);
    const n = v.length;
    if (!n) return { n: 0 };
    const m = v.reduce((s, x) => s + x, 0) / n;
    const sd = n > 1 ? Math.sqrt(v.reduce((s, x) => s + (x - m) * (x - m), 0) / (n - 1)) : 0;
    const q = (p) => { const i = p * (n - 1), lo = Math.floor(i), hi = Math.ceil(i); return v[lo] + (v[hi] - v[lo]) * (i - lo); };
    return { n, media: m, desvio: sd, min: v[0], q25: q(0.25), mediana: q(0.5), q75: q(0.75), max: v[n - 1] };
  }
  return {
    cholesky, resolverInferior, resolverSuperiorT, logDetCholesky, matVec, dot, inversaPequena, choleskyPequena,
    nelderMead, minimizar, hessiana, covarianciaLaplace, mcmc, lnGamma, gamaQ, pQuiQuadrado, erfc, sigmaDeP, descrever,
  };
})();

/* ── 05 · motor cosmológico ────────────────────────────────────────────────
 * Constantes físicas com fonte; nenhum parâmetro cosmológico embutido.
 * E(z) para w0waCDM (CPL) com curvatura e radiação opcionais; distâncias por
 * Simpson em células de Δz e interpolação de Hermite com a derivada exata 1/E.
 */
const COSMO = (() => {
  const C_KMS = 299792.458;               // definição do SI
  const C_MS = 299792458;                 // definição do SI
  const SIGMA_SB = 5.670374419e-8;        // CODATA 2018, W m⁻² K⁻⁴
  const G_SI = 6.67430e-11;               // CODATA 2018, m³ kg⁻¹ s⁻²
  const MPC_M = 3.0856775814913673e22;    // IAU 2015 (pc exato × 10⁶)
  const T_CMB = 2.7255;                   // K, Fixsen 2009, ApJ 707, 916
  const NEFF = 3.044;                     // espécies relativísticas, Modelo Padrão
  const H100_SI = 1e5 / MPC_M;
  const RHO_CRIT_H2 = 3 * H100_SI * H100_SI / (8 * Math.PI * G_SI);
  const OMEGA_GAMMA_H2 = (4 * SIGMA_SB * Math.pow(T_CMB, 4) / Math.pow(C_MS, 3)) / RHO_CRIT_H2;
  const FATOR_NU = (7 / 8) * Math.pow(4 / 11, 4 / 3);
  const OMEGA_R_H2 = OMEGA_GAMMA_H2 * (1 + FATOR_NU * NEFF);
  const GYR_S = 365.25 * 86400 * 1e9;
  const TEMPO_HUBBLE_H1_GYR = (1 / H100_SI) / GYR_S;  // 1/H0 para h = 1, em Gyr

  function E2(z, p) {
    const zp = 1 + z, a = 1 / zp;
    const Or = p.Or || 0, Ok = p.Ok || 0;
    const w0 = p.w0 === undefined ? -1 : p.w0, wa = p.wa || 0;
    const Ode = 1 - p.Om - Or - Ok;
    let fde = 1;
    if (w0 !== -1 || wa !== 0) fde = Math.pow(a, -3 * (1 + w0 + wa)) * Math.exp(-3 * wa * (1 - a));
    return p.Om * zp * zp * zp + Or * zp * zp * zp * zp + Ok * zp * zp + Ode * fde;
  }
  const E = (z, p) => Math.sqrt(E2(z, p));

  /* Tabela de χ(z) = ∫ dz/E em unidades de c/H0. */
  function tabela(p, zmax, dz = 0.005) {
    const n = Math.ceil(zmax / dz) + 2;
    const z = new Float64Array(n), chi = new Float64Array(n), inv = new Float64Array(n);
    for (let i = 0; i < n; i++) {
      z[i] = i * dz;
      const e2 = E2(z[i], p);
      if (!(e2 > 0) || !Number.isFinite(e2)) return null;
      inv[i] = 1 / Math.sqrt(e2);
    }
    for (let i = 1; i < n; i++) {
      const e2m = E2(z[i - 1] + dz / 2, p);
      if (!(e2m > 0)) return null;
      chi[i] = chi[i - 1] + (dz / 6) * (inv[i - 1] + 4 / Math.sqrt(e2m) + inv[i]);
    }
    return { dz, z, chi, inv, n, p };
  }
  function chi(t, zq) {
    if (zq <= 0) return 0;
    if (zq > t.z[t.n - 1]) throw new Error('z=' + zq + ' além da tabela (zmax=' + t.z[t.n - 1] + ')');
    const i = Math.min(t.n - 2, Math.floor(zq / t.dz));
    const h = t.dz, s = (zq - t.z[i]) / h, s2 = s * s, s3 = s2 * s;
    return (2 * s3 - 3 * s2 + 1) * t.chi[i] + (s3 - 2 * s2 + s) * h * t.inv[i] + (-2 * s3 + 3 * s2) * t.chi[i + 1] + (s3 - s2) * h * t.inv[i + 1];
  }
  /* Distância comóvel transversal em unidades de c/H0. */
  function transversal(t, zq) {
    const x = chi(t, zq), Ok = t.p.Ok || 0;
    if (Math.abs(Ok) < 1e-12) return x;
    const r = Math.sqrt(Math.abs(Ok));
    return Ok > 0 ? Math.sinh(r * x) / r : Math.sin(r * x) / r;
  }
  /* Razões BAO para hrd = h·r_d (Mpc). */
  function baoRazao(qtd, zq, t, hrd) {
    const fator = C_KMS / (100 * hrd);
    const dm = fator * transversal(t, zq);
    const dh = fator / E(zq, t.p);
    if (qtd === 'DM_over_rs') return dm;
    if (qtd === 'DH_over_rs') return dh;
    if (qtd === 'DV_over_rs') return Math.cbrt(zq * dm * dm * dh);
    throw new Error('observável BAO desconhecido: ' + qtd);
  }
  /* Módulo de distância: D_L = (1+z_hel)·D_M(z_cmb), em Mpc, com H0 em km/s/Mpc. */
  function modulo(zcmb, zhel, t, H0) {
    const dl = (1 + zhel) * (C_KMS / H0) * transversal(t, zcmb);
    return 5 * Math.log10(dl) + 25;
  }
  /* Idade: ΛCDM plano analítico e integral geral em a. */
  function idadeLCDM(H0, Om) {
    const OL = 1 - Om;
    return (2 / (3 * Math.sqrt(OL))) * Math.asinh(Math.sqrt(OL / Om)) * TEMPO_HUBBLE_H1_GYR / (H0 / 100);
  }
  function idadeNumerica(H0, p, n = 20000) {
    const f = (a) => (a <= 0 ? 0 : 1 / (a * Math.sqrt(E2(1 / a - 1, p))));
    const h = 1 / n;
    let s = f(1e-12) + f(1);
    for (let i = 1; i < n; i++) s += (i % 2 ? 4 : 2) * f(i * h);
    return (h / 3) * s * TEMPO_HUBBLE_H1_GYR / (H0 / 100);
  }
  const rhoCritico = (H0) => RHO_CRIT_H2 * (H0 / 100) * (H0 / 100);
  /* Aubourg et al. 2015, eq. 16 (arXiv:1411.1074): precisão de 0,021% para
     Neff = 3,046, Σmν < 0,6 eV e ω_b, ω_cb dentro de 3σ do Planck. */
  function rdAubourg(omb, omcb, somaMnuEv = 0.06) {
    const omnu = 0.0107 * somaMnuEv;
    return 55.154 * Math.exp(-72.3 * (omnu + 0.0006) * (omnu + 0.0006)) / (Math.pow(omcb, 0.25351) * Math.pow(omb, 0.12807));
  }
  const S8de = (sigma8, Om) => sigma8 * Math.sqrt(Om / 0.3);
  const OrDe = (h) => OMEGA_R_H2 / (h * h);
  return {
    C_KMS, T_CMB, NEFF, OMEGA_GAMMA_H2, OMEGA_R_H2, TEMPO_HUBBLE_H1_GYR, RHO_CRIT_H2,
    E2, E, tabela, chi, transversal, baoRazao, modulo, idadeLCDM, idadeNumerica, rhoCritico, rdAubourg, S8de, OrDe,
  };
})();

/* ── 06 · dados: download com cache e leitores de formato ──────────────────
 * Leitores sem dependência: tabela de espaços, SNANA (VARNAMES/SN:), CSV,
 * VOTable TABLEDATA, NPY, NPZ (zip com deflate), FITS BINTABLE (com gzip),
 * covariância do Pantheon+ e índice de diretório HTTP.
 */
const DADOS = (() => {
  const memoria = new Map();
  function dirCache() {
    const d = path.join(CFG.DATA_DIR, 'cache');
    try { fs.mkdirSync(d, { recursive: true }); } catch (e) { /* disco somente leitura: segue só em memória */ }
    return d;
  }
  /* Lê o corpo de uma resposta fetch por partes, parando no limite de bytes (sem content-length, chunked, o limite vale do mesmo jeito). */
  async function lerCorpo(r, maxBytes, rotulo) {
    const partes = [];
    let total = 0;
    if (!r.body) return Buffer.alloc(0);
    for await (const pedaco of r.body) {
      total += pedaco.length;
      if (total > maxBytes) { try { await r.body.cancel(); } catch (e) { /* já encerrado */ } throw new Error(rotulo + ' excede o limite de ' + maxBytes + ' bytes'); }
      partes.push(Buffer.from(pedaco));
    }
    return Buffer.concat(partes, total);
  }
  async function baixar(url, opcoes = {}) {
    if (memoria.has(url)) return memoria.get(url);
    const maxBytes = opcoes.maxBytes || 300 * 1024 * 1024;
    const arq = path.join(dirCache(), U.sha256(url).slice(0, 32));
    let buf = null, origem = 'rede';
    try { if (fs.existsSync(arq)) { buf = fs.readFileSync(arq); origem = 'cache em disco'; } } catch (e) { buf = null; }
    if (!buf) {
      let r;
      try { r = await fetch(url, { signal: AbortSignal.timeout(opcoes.timeout || CFG.TIMEOUT_DADOS_MS), headers: { 'user-agent': 'parallax-celulas/' + VERSAO } }); }
      catch (e) { throw new Error('falha de rede ao baixar ' + url + ': ' + U.causa(e)); }
      if (!r.ok) { try { await r.body.cancel(); } catch (e) { /* */ } throw new Error('HTTP ' + r.status + ' ao baixar ' + url); }
      const tam = Number(r.headers.get('content-length') || 0);
      if (tam && tam > maxBytes) { try { await r.body.cancel(); } catch (e) { /* */ } throw new Error('arquivo de ' + tam + ' bytes excede o limite de ' + maxBytes); }
      try { buf = await lerCorpo(r, maxBytes, 'arquivo ' + url); } catch (e) { throw new Error('download interrompido de ' + url + ': ' + U.causa(e)); }
      if (!opcoes.semDisco) { try { fs.writeFileSync(arq, buf); } catch (e) { /* sem disco: só memória */ } }
    }
    const reg = { url, buf, sha256: U.sha256(buf), bytes: buf.length, quando: new Date().toISOString(), origem };
    if (!opcoes.semMemoria) memoria.set(url, reg); // arquivos grandes de índice não ficam retidos
    return reg;
  }
  const texto = (buf) => (buf[0] === 0x1f && buf[1] === 0x8b ? zlib.gunzipSync(buf) : buf).toString('utf8');

  /* Tabela separada por espaços; primeira linha não comentada é o cabeçalho
     (ou cabecalho explícito). */
  function espacos(t, opcoes = {}) {
    const linhas = t.split(/\r?\n/).filter((l) => l.trim() && !l.trim().startsWith(opcoes.comentario || '#'));
    let colunas = opcoes.colunas;
    if (!colunas) colunas = linhas.shift().trim().split(/\s+/);
    const dados = linhas.map((l) => l.trim().split(/\s+/));
    return { colunas, linhas: dados.map((v) => v.map(numOuTexto)) };
  }
  /* Formato SNANA: "VARNAMES: a b c" e linhas "SN: ...". */
  function snana(t) {
    let colunas = null;
    const linhas = [];
    for (const l of t.split(/\r?\n/)) {
      const s = l.trim();
      if (s.startsWith('VARNAMES:')) colunas = s.slice(9).trim().split(/\s+/);
      else if (s.startsWith('SN:')) linhas.push(s.slice(3).trim().split(/\s+/).map(numOuTexto));
    }
    if (!colunas) throw new Error('SNANA: linha VARNAMES ausente');
    return { colunas, linhas };
  }
  function numOuTexto(v) {
    if (v === '' || v === undefined) return null;
    const n = Number(v);
    return Number.isFinite(n) && /^[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$/.test(String(v).trim()) ? n : v;
  }
  function csv(t, sep = ',', maxLinhas = Infinity) {
    const linhas = [];
    let campo = '', linha = [], aspas = false, truncado = false;
    for (let i = 0; i < t.length; i++) {
      const c = t[i];
      if (aspas) {
        if (c === '"') { if (t[i + 1] === '"') { campo += '"'; i++; } else aspas = false; }
        else campo += c;
      } else if (c === '"') aspas = true;
      else if (c === sep) { linha.push(campo); campo = ''; }
      else if (c === '\n' || c === '\r') {
        if (c === '\r' && t[i + 1] === '\n') i++;
        linha.push(campo); campo = '';
        if (linha.length > 1 || linha[0] !== '') linhas.push(linha);
        linha = [];
        if (linhas.length > maxLinhas) { truncado = true; break; }
      } else campo += c;
    }
    if (!truncado && (campo !== '' || linha.length)) { linha.push(campo); linhas.push(linha); }
    const colunas = (linhas.shift() || []).map((s) => s.trim());
    const out = { colunas, linhas: linhas.slice(0, maxLinhas).map((l) => l.map((v) => numOuTexto(v.trim()))) };
    if (truncado) out.truncado = maxLinhas;
    return out;
  }
  /* VOTable mínimo: FIELD@name e TABLEDATA. Detecta QUERY_STATUS=ERROR. */
  function votable(t) {
    const erro = /<INFO[^>]*name="QUERY_STATUS"[^>]*value="ERROR"[^>]*>([\s\S]*?)<\/INFO>/i.exec(t) || /<INFO[^>]*value="ERROR"[^>]*name="QUERY_STATUS"[^>]*>([\s\S]*?)<\/INFO>/i.exec(t);
    if (erro) throw new Error('TAP: ' + decodificarXML(erro[1].trim()).slice(0, 500));
    const colunas = [...t.matchAll(/<FIELD\b[^>]*\bname="([^"]*)"/gi)].map((m) => decodificarXML(m[1]));
    const linhas = [];
    for (const tr of t.matchAll(/<TR\b[^>]*>([\s\S]*?)<\/TR>/gi)) {
      const tds = [...tr[1].matchAll(/<TD\b[^>]*?(?:\/>|>([\s\S]*?)<\/TD>)/gi)].map((m) => numOuTexto(decodificarXML((m[1] || '').trim())));
      linhas.push(tds);
    }
    return { colunas, linhas };
  }
  function decodificarXML(s) {
    return String(s).replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&amp;/g, '&');
  }
  /* NPY v1/v2/v3: devolve { dtype, forma, dados }. */
  function npy(buf) {
    if (buf[0] !== 0x93 || buf.toString('latin1', 1, 6) !== 'NUMPY') throw new Error('NPY: assinatura inválida');
    const maior = buf[6];
    const lenCab = maior === 1 ? buf.readUInt16LE(8) : buf.readUInt32LE(8);
    const ini = maior === 1 ? 10 : 12;
    const cab = buf.toString('latin1', ini, ini + lenCab);
    const descr = /'descr'\s*:\s*'([^']+)'/.exec(cab)[1];
    const fortran = /'fortran_order'\s*:\s*True/.test(cab);
    const forma = (/'shape'\s*:\s*\(([^)]*)\)/.exec(cab)[1].split(',').map((s) => s.trim()).filter(Boolean).map(Number));
    if (fortran && forma.length > 1) throw new Error('NPY: ordem Fortran não suportada');
    const corpo = buf.subarray(ini + lenCab);
    const total = forma.reduce((a, b) => a * b, 1);
    const le = descr[0] === '<' || descr[0] === '|' || descr[0] === '=';
    const tipo = descr.slice(1);
    const tam = { f8: 8, f4: 4, i8: 8, i4: 4, i2: 2, u1: 1, b1: 1 }[tipo];
    if (!tam) throw new Error('NPY: dtype não suportado ' + descr);
    const out = tipo.startsWith('f') ? new Float64Array(total) : new Array(total);
    for (let i = 0; i < total; i++) {
      const o = i * tam;
      let v;
      if (tipo === 'f8') v = le ? corpo.readDoubleLE(o) : corpo.readDoubleBE(o);
      else if (tipo === 'f4') v = le ? corpo.readFloatLE(o) : corpo.readFloatBE(o);
      else if (tipo === 'i8') v = Number(le ? corpo.readBigInt64LE(o) : corpo.readBigInt64BE(o));
      else if (tipo === 'i4') v = le ? corpo.readInt32LE(o) : corpo.readInt32BE(o);
      else if (tipo === 'i2') v = le ? corpo.readInt16LE(o) : corpo.readInt16BE(o);
      else v = corpo[o];
      out[i] = tipo === 'b1' ? !!v : v;
    }
    return { dtype: descr, forma, dados: out };
  }
  /* ZIP (para .npz): diretório central, métodos 0 (stored) e 8 (deflate). */
  function zip(buf) {
    let fim = -1;
    for (let i = buf.length - 22; i >= Math.max(0, buf.length - 65557); i--) if (buf.readUInt32LE(i) === 0x06054b50) { fim = i; break; }
    if (fim < 0) throw new Error('ZIP: fim do diretório central não encontrado');
    const n = buf.readUInt16LE(fim + 10);
    let p = buf.readUInt32LE(fim + 16);
    const arquivos = {};
    for (let k = 0; k < n; k++) {
      if (buf.readUInt32LE(p) !== 0x02014b50) throw new Error('ZIP: entrada central inválida');
      const metodo = buf.readUInt16LE(p + 10);
      let comp = buf.readUInt32LE(p + 20), orig = buf.readUInt32LE(p + 24);
      const lNome = buf.readUInt16LE(p + 28), lExtra = buf.readUInt16LE(p + 30), lCom = buf.readUInt16LE(p + 32);
      let local = buf.readUInt32LE(p + 42);
      const nome = buf.toString('utf8', p + 46, p + 46 + lNome);
      // ZIP64: tamanhos/offset em campo extra 0x0001
      if (comp === 0xffffffff || orig === 0xffffffff || local === 0xffffffff) {
        let e = p + 46 + lNome;
        const fimExtra = e + lExtra;
        while (e < fimExtra) {
          const id = buf.readUInt16LE(e), tamCampo = buf.readUInt16LE(e + 2);
          if (id === 0x0001) {
            let q = e + 4;
            if (orig === 0xffffffff) { orig = Number(buf.readBigUInt64LE(q)); q += 8; }
            if (comp === 0xffffffff) { comp = Number(buf.readBigUInt64LE(q)); q += 8; }
            if (local === 0xffffffff) { local = Number(buf.readBigUInt64LE(q)); q += 8; }
          }
          e += 4 + tamCampo;
        }
      }
      if (buf.readUInt32LE(local) !== 0x04034b50) throw new Error('ZIP: cabeçalho local inválido');
      const ini = local + 30 + buf.readUInt16LE(local + 26) + buf.readUInt16LE(local + 28);
      const bruto = buf.subarray(ini, ini + comp);
      let dados;
      if (metodo === 0) dados = bruto;
      else if (metodo === 8) dados = zlib.inflateRawSync(bruto);
      else throw new Error('ZIP: método ' + metodo + ' não suportado');
      if (dados.length !== orig) throw new Error('ZIP: tamanho descompactado divergente em ' + nome);
      arquivos[nome] = dados;
      p += 46 + lNome + lExtra + lCom;
    }
    return arquivos;
  }
  function npz(buf) {
    const arqs = zip(buf), out = {};
    for (const [nome, b] of Object.entries(arqs)) out[nome.replace(/\.npy$/, '')] = npy(b);
    return out;
  }
  /* FITS BINTABLE (primeira extensão de tabela binária). */
  function fits(bufBruto, maxLinhas = Infinity) {
    const buf = bufBruto[0] === 0x1f && bufBruto[1] === 0x8b ? zlib.gunzipSync(bufBruto) : bufBruto;
    let p = 0;
    function lerCabecalho() {
      const cards = {};
      let fimAchado = false;
      while (!fimAchado) {
        if (p + 2880 > buf.length) throw new Error('FITS: cabeçalho truncado');
        for (let i = 0; i < 36; i++) {
          const card = buf.toString('latin1', p + i * 80, p + i * 80 + 80);
          const chave = card.slice(0, 8).trim();
          if (chave === 'END') { fimAchado = true; break; }
          if (card[8] === '=' ) {
            let v = card.slice(10).trim();
            if (v.startsWith("'")) { const m = /^'((?:[^']|'')*)'/.exec(v); v = m ? m[1].replace(/''/g, "'").trimEnd() : v; }
            else { v = v.split('/')[0].trim(); if (v === 'T') v = true; else if (v === 'F') v = false; else if (v !== '' && Number.isFinite(Number(v))) v = Number(v); }
            cards[chave] = v;
          }
        }
        p += 2880;
      }
      return cards;
    }
    const prim = lerCabecalho();
    let tamDados = 0;
    if (prim.NAXIS > 0) { tamDados = Math.abs(prim.BITPIX) / 8; for (let i = 1; i <= prim.NAXIS; i++) tamDados *= prim['NAXIS' + i]; }
    p += Math.ceil(tamDados / 2880) * 2880;
    while (p < buf.length) {
      const h = lerCabecalho();
      const largura = h.NAXIS1 || 0, nLinhas = h.NAXIS2 || 0;
      const bytesTabela = largura * nLinhas + (h.PCOUNT || 0);
      if (h.XTENSION === 'BINTABLE') {
        const campos = [];
        let off = 0;
        for (let i = 1; i <= h.TFIELDS; i++) {
          const m = /^(\d*)([LXBIJKAEDCMPQ])/.exec(String(h['TFORM' + i]).trim());
          if (!m) throw new Error('FITS: TFORM inválido ' + h['TFORM' + i]);
          const rep = m[1] === '' ? 1 : Number(m[1]), tipo = m[2];
          const tam = { L: 1, B: 1, I: 2, J: 4, K: 8, A: 1, E: 4, D: 8, C: 8, M: 16, P: 8, Q: 16 }[tipo];
          const bytes = tipo === 'X' ? Math.ceil(rep / 8) : rep * tam;
          campos.push({ nome: String(h['TTYPE' + i] || 'COL' + i).trim(), tipo, rep, off, escala: h['TSCAL' + i] ?? 1, zero: h['TZERO' + i] ?? 0, unidade: h['TUNIT' + i] || '' });
          off += bytes;
        }
        const linhas = [];
        const nLer = Math.min(nLinhas, maxLinhas);
        for (let r = 0; r < nLer; r++) {
          const base = p + r * largura;
          linhas.push(campos.map((c) => lerCampo(buf, base + c.off, c)));
        }
        const out = { colunas: campos.map((c) => c.nome), unidades: campos.map((c) => c.unidade), linhas, extensao: h.EXTNAME || '', linhasNoArquivo: nLinhas };
        if (nLer < nLinhas) out.truncado = maxLinhas;
        return out;
      }
      p += Math.ceil(bytesTabela / 2880) * 2880;
    }
    throw new Error('FITS: nenhuma extensão BINTABLE');
  }
  function lerCampo(buf, o, c) {
    const lerUm = (q) => {
      switch (c.tipo) {
        case 'L': return buf[q] === 84;
        case 'B': return buf[q] * c.escala + c.zero;
        case 'I': return buf.readInt16BE(q) * c.escala + c.zero;
        case 'J': return buf.readInt32BE(q) * c.escala + c.zero;
        case 'K': return Number(buf.readBigInt64BE(q)) * c.escala + c.zero;
        case 'E': return buf.readFloatBE(q) * c.escala + c.zero;
        case 'D': return buf.readDoubleBE(q) * c.escala + c.zero;
        default: return null;
      }
    };
    if (c.tipo === 'A') return buf.toString('latin1', o, o + c.rep).replace(/\0.*$/, '').trim();
    if (c.tipo === 'X' || c.tipo === 'P' || c.tipo === 'Q' || c.tipo === 'C' || c.tipo === 'M') return null;
    const tam = { L: 1, B: 1, I: 2, J: 4, K: 8, E: 4, D: 8 }[c.tipo];
    if (c.rep === 1) return lerUm(o);
    const v = [];
    for (let i = 0; i < Math.min(c.rep, 64); i++) v.push(lerUm(o + i * tam));
    return v;
  }
  /* Covariância do Pantheon+: N na primeira linha e N² valores. */
  function covPantheon(t) {
    // varredura token a token: evita um array de ~2,9 milhões de strings
    let i = 0;
    const L = t.length;
    const prox = () => { while (i < L && t.charCodeAt(i) <= 32) i++; const ini = i; while (i < L && t.charCodeAt(i) > 32) i++; return ini < i ? t.slice(ini, i) : null; };
    const n = Number(prox());
    if (!Number.isInteger(n) || n <= 0) throw new Error('covariância: primeira linha não é N');
    const C = new Float64Array(n * n);
    for (let k = 0; k < n * n; k++) {
      const s = prox();
      if (s === null) throw new Error('covariância: esperados ' + (n * n) + ' valores, achados ' + k);
      const v = Number(s);
      if (!Number.isFinite(v)) throw new Error('covariância: valor inválido na posição ' + k + ': ' + s.slice(0, 30));
      C[k] = v;
    }
    if (prox() !== null) throw new Error('covariância: mais de ' + (n * n) + ' valores');
    return { n, C };
  }
  /* Índice de diretório HTTP (Apache/nginx): links relativos. */
  function indice(t, base) {
    const links = [...t.matchAll(/href="([^"?#]+)"/gi)].map((m) => m[1]).filter((h) => !h.startsWith('/') && !h.startsWith('..') && !/^[a-z]+:/i.test(h));
    return [...new Set(links)].map((h) => new URL(h, base).toString());
  }
  return { baixar, lerCorpo, texto, espacos, snana, csv, votable, npy, zip, npz, fits, covPantheon, indice, numOuTexto, memoria };
})();

/* ── 07 · TAP (ADQL síncrono) ──────────────────────────────────────────────
 * Endpoints públicos. "conferido" diz como o endereço foi confirmado; os demais
 * são testados ao vivo por /api/testar no próprio Render.
 */
const TAP = (() => {
  const SERVICOS = {
    gaia: { url: 'https://gea.esac.esa.int/tap-server/tap', nome: 'ESA Gaia Archive', conferido: 'endpoint documentado em cosmos.esa.int/web/gaia-users/archive/programmatic-access' },
    irsa: { url: 'https://irsa.ipac.caltech.edu/TAP', nome: 'NASA/IPAC IRSA (Euclid Q1, SPHEREx e outros)', conferido: 'documento VOSI capabilities aberto em 16/out/2026' },
    casda: { url: 'https://casda.csiro.au/casda_vo_tools/tap', nome: 'CSIRO CASDA (ASKAP: WALLABY, EMU, RACS)', conferido: 'listado em research.csiro.au/casda/services' },
    vizier: { url: 'https://tapvizier.cds.unistra.fr/TAPVizieR/tap', nome: 'CDS VizieR (catálogos publicados)', conferido: 'não conferido daqui (robots); testar em /api/testar' },
    simbad: { url: 'https://simbad.cds.unistra.fr/simbad/sim-tap', nome: 'CDS SIMBAD', conferido: 'não conferido daqui; testar em /api/testar' },
    heasarc: { url: 'https://heasarc.gsfc.nasa.gov/xamin/vo/tap', nome: 'NASA HEASARC (Chandra, XMM-Newton, Fermi)', conferido: 'não conferido daqui (robots); testar em /api/testar' },
    datalab: { url: 'https://datalab.noirlab.edu/tap', nome: 'NOIRLab Astro Data Lab (DES DR2, DESI DR1)', conferido: 'capabilities aberto em 16/out/2026' },
  };
  const MAX_BYTES = 30 * 1024 * 1024; // resposta TAP lida por partes até este limite
  async function consultar(servico, adql, maxLinhas = 2000) {
    const s = SERVICOS[servico];
    if (!s) throw new Error('serviço TAP desconhecido: ' + servico);
    if (!/^\s*select\b/i.test(adql)) throw new Error('somente consultas SELECT são aceitas');
    const corpo = new URLSearchParams({ REQUEST: 'doQuery', LANG: 'ADQL', FORMAT: 'csv', RESPONSEFORMAT: 'csv', MAXREC: String(Math.min(maxLinhas, 50000)), QUERY: adql });
    const t0 = Date.now();
    let r, bruto;
    try {
      r = await fetch(s.url + '/sync', {
        method: 'POST', body: corpo,
        headers: { 'content-type': 'application/x-www-form-urlencoded', 'user-agent': 'parallax-celulas/' + VERSAO },
        signal: AbortSignal.timeout(CFG.TIMEOUT_DADOS_MS),
      });
      bruto = await DADOS.lerCorpo(r, MAX_BYTES, 'resposta TAP');
    } catch (e) { throw new Error('TAP ' + servico + ': falha de rede: ' + U.causa(e)); }
    const txt = bruto.toString('utf8');
    const ehXML = /^\s*<\?xml|<VOTABLE/i.test(txt.slice(0, 500));
    if (!r.ok && !ehXML) throw new Error('TAP ' + servico + ': HTTP ' + r.status + ' ' + txt.slice(0, 300));
    const tabela = ehXML ? DADOS.votable(txt) : DADOS.csv(txt);
    return { servico, url: s.url, adql, ms: Date.now() - t0, sha256: U.sha256(bruto), bytes: bruto.length, ...tabela };
  }
  async function testar(servico) {
    const s = SERVICOS[servico];
    const t0 = Date.now();
    let r, txt;
    try {
      r = await fetch(s.url + '/capabilities', { signal: AbortSignal.timeout(20000), headers: { 'user-agent': 'parallax-celulas/' + VERSAO } });
      txt = await r.text();
    } catch (e) { throw new Error('falha de rede: ' + U.causa(e)); }
    return { ok: r.ok && /capabilit/i.test(txt), status: r.status, ms: Date.now() - t0 };
  }
  return { SERVICOS, consultar, testar };
})();

/* ── 08 · registro de proveniência (ledger) e guarda de números ─────────────
 * Etiquetas (mesma doutrina do UROBOROS):
 *   [D] dado: arquivo oficial, resposta TAP ou valor publicado (literatura)
 *   [C] calculado aqui, de forma determinística, a partir de entradas [D]
 *   [F] fato verificado (texto com fonte)
 *   [L] lacuna: o dado não existe ou não está acessível
 * Todo número que um agente escreve precisa existir no registro (ou ter vindo
 * de uma ferramenta na própria célula). Par "valor ± incerteza" (ou "+σ₊/−σ₋")
 * precisa vir inteiro de um MESMO registro: coincidência numérica isolada não
 * basta para aprovar uma medida.
 */
const LEDGER = (() => {
  const entradas = new Map();
  const GRANDEZAS_PUBLICAVEIS = {
    H0: { rotulo: 'H₀', unidade: 'km/s/Mpc' }, Om: { rotulo: 'Ω_m', unidade: '' }, S8: { rotulo: 'S₈', unidade: '' },
    sigma8: { rotulo: 'σ₈', unidade: '' }, w: { rotulo: 'w', unidade: '' }, w0: { rotulo: 'w₀', unidade: '' }, wa: { rotulo: 'w_a', unidade: '' },
    hrd: { rotulo: 'h·r_d', unidade: 'Mpc' }, rd: { rotulo: 'r_d', unidade: 'Mpc' }, ombh2: { rotulo: 'ω_b', unidade: '' },
    omch2: { rotulo: 'ω_c', unidade: '' }, theta100: { rotulo: '100θ*', unidade: '' }, M: { rotulo: 'M (SN Ia)', unidade: 'mag' },
  };
  function numerosDe(e) {
    const ns = [];
    for (const k of ['valor', 'sigma', 'sigma_mais', 'sigma_menos', 'chi2', 'n', 'ndof', 'p', 'z']) if (typeof e[k] === 'number' && Number.isFinite(e[k])) ns.push(e[k]);
    if (Array.isArray(e.numeros)) for (const v of e.numeros) if (typeof v === 'number' && Number.isFinite(v)) ns.push(v);
    // texto que faz parte do registro (fato, rótulo da fonte, método, premissas, avisos) também pode ser citado
    const textos = [e.texto, e.rotulo, e.metodo, ...(Array.isArray(e.premissas) ? e.premissas : []), ...(Array.isArray(e.avisos) ? e.avisos : [])];
    for (const t of textos) if (typeof t === 'string') for (const c of extrairNumeros(t)) ns.push(...c.valores);
    if (e.fonte && typeof e.fonte.url === 'string') ns.push(...idsArxiv(e.fonte.url));
    return ns;
  }
  // de URLs só se aproveita o identificador arXiv (aaaa.nnnnn); hashes e caminhos ficam de fora
  const idsArxiv = (u) => [...String(u).matchAll(/(?:^|[^\d.])(\d{4}\.\d{4,5})(?![\d.])/g)].map((m) => Number(m[1]));
  /* Números de um resultado de ferramenta (JSON): valores numéricos e números dentro de textos. */
  function numerosDeJSON(x, limite = 20000) {
    const out = [];
    const visitar = (v, prof) => {
      if (out.length >= limite || prof > 12) return;
      if (typeof v === 'number') { if (Number.isFinite(v)) out.push(v); }
      else if (typeof v === 'string') { if (/^https?:\/\//.test(v)) out.push(...idsArxiv(v)); else for (const c of extrairNumeros(v)) out.push(...c.valores); }
      else if (Array.isArray(v)) { for (const y of v) visitar(y, prof + 1); }
      else if (v && typeof v === 'object') { for (const y of Object.values(v)) visitar(y, prof + 1); }
    };
    visitar(x, 0);
    return out.length > limite ? out.slice(0, limite) : out;
  }
  function registrar(e) {
    const prefixo = e.tag === 'C' ? 'C' : e.tag === 'F' ? 'F' : e.fonte && e.fonte.tipo === 'tap' ? 'D-tap' : e.fonte && e.fonte.tipo === 'literatura' ? 'D-lit' : 'D-arq';
    const id = e.id || U.novoId(prefixo, JSON.stringify(e).slice(0, 400));
    const reg = { ...e, id, publicado: e.publicado || false, quando: e.quando || new Date().toISOString() };
    reg.numeros_guarda = numerosDe(reg);
    entradas.set(id, reg);
    return reg;
  }
  const obter = (id) => entradas.get(String(id).replace(/^\[|\]$/g, '').replace(/^(C|D|F):/, ''));
  function listar(f = {}) {
    return [...entradas.values()].filter((e) => (!f.celula || e.celula === f.celula || (f.incluirRede && e.publicado) || (f.incluirLiteratura && e.fonte && e.fonte.tipo === 'literatura'))
      && (!f.grandeza || e.grandeza === f.grandeza) && (!f.tag || e.tag === f.tag) && (f.publicado === undefined || e.publicado === f.publicado));
  }
  function publicar(id, celula) {
    const e = obter(id);
    if (!e) throw new Error('registro inexistente: ' + id);
    if (!e.grandeza || !GRANDEZAS_PUBLICAVEIS[e.grandeza]) throw new Error('só grandezas cosmológicas podem ir para a rede (' + Object.keys(GRANDEZAS_PUBLICAVEIS).join(', ') + ')');
    if (e.tag !== 'C' && e.tag !== 'D') throw new Error('só registros [C] ou [D] podem ser publicados');
    if (!(typeof e.valor === 'number' && Number.isFinite(e.valor))) throw new Error('registro sem valor numérico');
    if (!(sigmaSimetrico(e) > 0)) throw new Error('registro sem incerteza: não entra na rede');
    if (e.celula !== celula && !(e.fonte && e.fonte.tipo === 'literatura' && (e.celulas || []).includes(celula))) throw new Error('a célula ' + celula + ' só publica registros próprios ou literatura atribuída a ela');
    if (e.publicado && e.publicado_por !== celula) {
      // literatura atribuída a duas células, já publicada pela outra: cada célula fica com a própria cópia na rede
      const ja = [...entradas.values()].find((x) => x.copia_de === e.id && x.publicado_por === celula);
      if (ja) return ja;
      const copia = registrar({ ...e, id: undefined, numeros_guarda: undefined, celula, celulas: [celula], copia_de: e.id, publicado: true, publicado_por: celula, quando: undefined });
      return copia;
    }
    e.publicado = true;
    e.publicado_por = celula;
    return e;
  }
  const sigmaSimetrico = (e) => (e.sigma > 0 ? e.sigma : (e.sigma_mais > 0 && e.sigma_menos > 0 ? (e.sigma_mais + e.sigma_menos) / 2 : NaN));
  /* σ do lado voltado para o outro valor (assimétrico). */
  function sigmaLado(e, outroValor) {
    if (e.sigma > 0) return e.sigma;
    if (outroValor > e.valor) return e.sigma_mais;
    return e.sigma_menos;
  }

  /* ── guarda de números ─────────────────────────────────────────────────── */
  // número = dígitos não colados a letra/dígito/_ antes nem depois (assim "100θ*", "3x2pt", "DR2" e
  // "C01" não são números); σ e % podem vir colados depois ("5,4σ", "95%")
  const RE_NUM = /(^|[^\p{L}\p{N}_\-:./#])([-−]?\d+(?:[.,]\d+)*(?:[eE][-+−]?\d+)?)(?!(?![σ%])[\p{L}\p{N}_])/gu;
  function interpretar(s) {
    const t = s.replace('−', '-');
    const neg = t.startsWith('-') ? -1 : 1;
    const u = t.replace(/^[-+]/, '');
    const out = [];
    const exp = /[eE]/.test(u) ? u.slice(u.search(/[eE]/)) : '';
    const base = exp ? u.slice(0, u.search(/[eE]/)) : u;
    const e10 = exp ? Number(exp.slice(1).replace('−', '-')) : 0;
    const com = (v, casas) => out.push({ v: neg * v * Math.pow(10, e10), casas: Math.max(0, casas - e10) });
    if (base.includes(',') && base.includes('.')) {
      const ultimaVirg = base.lastIndexOf(','), ultimoPonto = base.lastIndexOf('.');
      if (ultimaVirg > ultimoPonto) { const d = base.split(',').pop().length; com(Number(base.replace(/\./g, '').replace(',', '.')), d); }
      else { const d = base.split('.').pop().length; com(Number(base.replace(/,/g, '')), d); }
    } else if (base.includes(',')) {
      if (/^[1-9]\d{0,2}(,\d{3})+$/.test(base)) com(Number(base.replace(/,/g, '')), 0);
      const partes = base.split(',');
      if (partes.length === 2) com(Number(partes[0] + '.' + partes[1]), partes[1].length);
    } else if (base.includes('.')) {
      if (/^[1-9]\d{0,2}(\.\d{3})+$/.test(base)) com(Number(base.replace(/\./g, '')), 0);
      const partes = base.split('.');
      if (partes.length === 2) com(Number(base), partes[1].length);
    } else com(Number(base), 0);
    return out.filter((c) => Number.isFinite(c.v));
  }
  // "+0,032/−0,033": a barra antes de um número com sinal é notação de erro assimétrico, não fração nem caminho
  const normalizar = (texto) => String(texto).replace(/\/(?=[-−+]\d)/g, '/ ');
  function extrairNumeros(texto) {
    const res = [];
    for (const m of normalizar(texto).matchAll(RE_NUM)) {
      const bruto = m[2];
      const pos = m.index + m[1].length;
      const cands = interpretar(bruto);
      if (cands.length) res.push({ bruto, pos, valores: cands.map((c) => c.v), cands });
    }
    return res;
  }
  function isento(c) {
    return c.cands.some(({ v, casas }) => casas === 0 && Number.isInteger(v) && ((Math.abs(v) <= 20) || (v >= 1990 && v <= 2040)));
  }
  function confere(cand, conjunto) {
    const tol = cand.casas > 0 ? 0.5 * Math.pow(10, -cand.casas) * (1 + 1e-9) : 0.5;
    for (const v of conjunto) {
      if (Math.abs(cand.v - v) <= tol || Math.abs(Math.abs(cand.v) - Math.abs(v)) <= tol) return true;
      if (cand.casas === 0 && Math.abs(v) >= 100 && Math.abs(cand.v - v) / Math.abs(v) < 0.005) return true;
    }
    return false;
  }
  /* Verifica um texto contra o escopo de registros visíveis para a célula. */
  // só a forma real dos ids ("F-score" ou "C-type" não contam): C-/D-arq-/D-tap-/D-lit- + 8 hex, F-<célula>-<n>, F-constantes
  const RE_ID = /\b(?:(?:C|D-arq|D-tap|D-lit)-[0-9a-f]{8}|F-[a-z]+-\d+|F-constantes)\b/gi;
  const RE_HASH_ANTES = /sha-?256\W{0,3}$|\bhash\W{0,3}$/i;
  const NIVEIS_CONFIANCA = [68, 68.3, 95, 95.4, 99, 99.7];
  function guarda(texto0, escopo) {
    const texto = normalizar(texto0);
    const ents = new Set(escopo.entradas);
    const citados = [...new Set((texto.match(RE_ID) || []).map((x) => x.toLowerCase().replace(/^f-/, 'F-').replace(/^c-/, 'C-').replace(/^d-/, 'D-')))];
    const inexistentes = [];
    for (const idc of citados) { const e = entradas.get(idc); if (e) ents.add(e); else inexistentes.push(idc); }
    const lista = [...ents];
    const conjunto = [];
    for (const e of lista) for (const v of (e.numeros_guarda || numerosDe(e))) conjunto.push(v);
    if (escopo.extras) for (const v of escopo.extras) conjunto.push(v);
    const achados = extrairNumeros(texto);
    const violacoes = [];
    let conferidos = 0, isentos = 0, pares = 0, gravidade = 0;
    // gravidade: medida inventada (valor ± σ) = 1; número solto ou id inexistente = 0,5
    const trecho = (a, b) => texto.slice(Math.max(0, a.pos - 50), Math.min(texto.length, b.pos + b.bruto.length + 50)).replace(/\s+/g, ' ');
    const tratados = new Set();
    // grupos "valor ± σ", "valor +σ₊/−σ₋" e "valor (+σ₊/−σ₋)": todos do mesmo registro
    for (let i = 0; i + 1 < achados.length; i++) {
      const a = achados[i], b = achados[i + 1], c3 = achados[i + 2];
      const entre = texto.slice(a.pos + a.bruto.length, b.pos);
      let grupo = null;
      if (/^\s*±\s*$/.test(entre)) grupo = [a, b];
      else if (/^\s*\(?\s*\+\s*$/.test(entre) && c3 && /^\s*\/\s*$/.test(texto.slice(b.pos + b.bruto.length, c3.pos))) grupo = [a, b, c3];
      if (!grupo) continue;
      grupo.forEach((x) => tratados.add(x));
      i += grupo.length - 1;
      if (grupo.every(isento)) { isentos += grupo.length; continue; }
      pares++;
      const ok = lista.some((e) => { const ns = e.numeros_guarda || numerosDe(e); return grupo.every((x) => isento(x) || x.cands.some((k) => confere(k, ns))); });
      if (ok) { conferidos += grupo.length; continue; }
      gravidade += 1;
      violacoes.push({ numero: grupo.map((x) => x.bruto).join(grupo.length === 2 ? ' ± ' : ' +/− '), trecho: trecho(a, grupo[grupo.length - 1]), motivo: 'valor e incerteza não vêm de um mesmo registro', gravidade: 1 });
    }
    for (const c of achados) {
      if (tratados.has(c)) continue;
      if (isento(c)) { isentos++; continue; }
      if (/^\s?%/.test(texto.slice(c.pos + c.bruto.length, c.pos + c.bruto.length + 2)) && c.cands.some((k) => NIVEIS_CONFIANCA.includes(k.v))) { isentos++; continue; } // nível de confiança (68%, 95%…)
      if (RE_HASH_ANTES.test(texto.slice(Math.max(0, c.pos - 16), c.pos))) { isentos++; continue; } // prefixo de SHA-256 só com dígitos
      if (c.cands.some((k) => confere(k, conjunto))) { conferidos++; continue; }
      gravidade += 0.5;
      violacoes.push({ numero: c.bruto, trecho: trecho(c, c), motivo: 'número sem registro', gravidade: 0.5 });
    }
    for (const idc of inexistentes) { gravidade += 0.5; violacoes.push({ numero: '[' + idc + ']', trecho: 'id citado que não existe no registro', motivo: 'id inexistente', gravidade: 0.5 }); }
    return { total: achados.length, conferidos, isentos, pares, violacoes, gravidade, citados: citados.length };
  }
  function limpar() { entradas.clear(); }
  return { registrar, obter, listar, publicar, sigmaSimetrico, sigmaLado, guarda, extrairNumeros, numerosDe, numerosDeJSON, GRANDEZAS_PUBLICAVEIS, limpar, entradas };
})();

/* ── 09 · literatura e fatos verificados ───────────────────────────────────
 * Cada valor abaixo foi conferido na página indicada em 16/out/2026. Nada aqui
 * vem de memória de modelo. dados_usados marca de quais conjuntos de dados o
 * valor depende, para o Polo V recusar combinações não independentes.
 */
const LIT = (() => {
  const L = [];
  const add = (celulas, base, itens) => itens.forEach(([grandeza, valor, s1, s2, extra]) => {
    // id estável (mesmo id em todo ciclo e em todo processo): derivado da fonte e da grandeza
    const e = { id: 'D-lit-' + U.sha256(base.rotulo + '|' + grandeza).slice(0, 8), tag: 'D', celulas, celula: celulas[0], grandeza, valor, unidade: (LEDGER.GRANDEZAS_PUBLICAVEIS[grandeza] || {}).unidade || '', ...base, ...(extra || {}) };
    if (s2 === undefined) e.sigma = s1; else { e.sigma_mais = s1; e.sigma_menos = s2; }
    L.push(e);
  });
  const lit = (rotulo, url, modelo, dados) => ({ rotulo, modelo, dados_usados: dados, fonte: { tipo: 'literatura', url, descricao: rotulo } });

  add(['planck'], lit('Planck 2018 VI, resumo (ΛCDM base)', 'https://arxiv.org/abs/1807.06209', 'ΛCDM base', ['planck']), [
    ['H0', 67.4, 0.5], ['Om', 0.315, 0.007], ['sigma8', 0.811, 0.006], ['ombh2', 0.0224, 0.0001], ['omch2', 0.120, 0.001], ['theta100', 1.0411, 0.0003],
  ]);
  add(['act'], lit('ACT DR6: P-ACT + lenteamento do CMB + DESI DR1 BAO (resumo)', 'https://arxiv.org/abs/2503.14452', 'ΛCDM', ['act', 'planck', 'desi']), [
    ['H0', 68.22, 0.36], ['ombh2', 0.0226, 0.0001], ['omch2', 0.118, 0.001], ['sigma8', 0.813, 0.005],
  ]);
  add(['act'], lit('ACT DR6: P-ACT + lenteamento + DESI DR2 BAO (resumo)', 'https://arxiv.org/abs/2503.14452', 'ΛCDM', ['act', 'planck', 'desi']), [['H0', 68.43, 0.27]]);
  add(['spt'], lit('SPT-3G D1 sozinho (PRD 113, 083504)', 'https://arxiv.org/abs/2506.20707', 'ΛCDM', ['spt']), [['H0', 66.66, 0.60]]);
  add(['hubble', 'gaia'], lit('SH0ES R22, Cefeidas + SN Ia (resumo)', 'https://arxiv.org/abs/2112.04510', 'escada de distâncias', ['shoes-cefeidas', 'pantheon+', 'gaia']), [['H0', 73.04, 1.04]]);
  add(['hubble'], lit('Pantheon+ só SNe, ΛCDM plano (resumo)', 'https://arxiv.org/abs/2202.04077', 'ΛCDM plano', ['pantheon+', 'cfa', 'foundation', 'des-sn-3yr']), [['Om', 0.334, 0.018]]);
  add(['hubble'], lit('Pantheon+ com SH0ES, w0CDM plano (resumo)', 'https://arxiv.org/abs/2202.04077', 'w0CDM plano', ['pantheon+', 'shoes-cefeidas', 'cfa', 'foundation', 'des-sn-3yr']), [['H0', 73.5, 1.1]]);
  add(['des'], lit('DES Y6 3x2pt, ΛCDM (resumo da apresentação)', 'https://indico-dpt.unige.ch/event/6/contributions/92/', 'ΛCDM', ['des-3x2pt']), [
    ['S8', 0.789, 0.012], ['Om', 0.333, 0.023, 0.028],
  ]);
  add(['des'], lit('DES Y6 3x2pt, wCDM (resumo do artigo)', 'https://arxiv.org/abs/2601.14559', 'wCDM', ['des-3x2pt']), [
    ['S8', 0.782, 0.021, 0.020], ['Om', 0.325, 0.032, 0.035], ['w', -1.12, 0.26, 0.20],
  ]);
  add(['des'], lit('DES-Dovekie só SNe, ΛCDM plano (resumo)', 'https://arxiv.org/abs/2511.07517', 'ΛCDM plano', ['des-sn', 'cfa', 'foundation']), [['Om', 0.330, 0.015]]);
  add(['des'], lit('DES-Dovekie + DESI DR2 + CMB (Planck, ACT, SPT), w0waCDM (resumo)', 'https://arxiv.org/abs/2511.07517', 'w0waCDM plano', ['des-sn', 'cfa', 'foundation', 'desi', 'planck', 'act', 'spt']), [
    ['w0', -0.803, 0.054], ['wa', -0.72, 0.21],
  ]);
  add(['subaru'], lit('HSC Y3 (Dalal et al. 2023)', 'https://arxiv.org/abs/2304.00701', 'ΛCDM', ['hsc']), [['S8', 0.776, 0.032, 0.033]]);
  add(['erosita'], lit('eRASS1, cosmologia de aglomerados (resumo)', 'https://arxiv.org/abs/2402.08458', 'ΛCDM', ['erosita', 'des-wl', 'kids', 'hsc']), [
    ['S8', 0.86, 0.01], ['Om', 0.29, 0.01, 0.02],
  ]);
  // VST/KiDS não é uma das 17 células: entra só como referência de comparação (nenhuma célula publica)
  add(['ref'], lit('KiDS-Legacy, cisalhamento cósmico (resumo)', 'https://arxiv.org/abs/2503.19441', 'ΛCDM', ['kids']), [['S8', 0.815, 0.016, 0.021]]);

  /* Fatos verificados por célula (texto com fonte; números entram na guarda). */
  const F = {
    desi: [
      ['DR1 público desde 19/mar/2025; do DR2 já saíram as cadeias e produtos cosmológicos, mas não os espectros.', 'https://data.desi.lbl.gov/doc/papers/'],
      ['O SPT-3G D1 relata diferença de 2,8σ entre CMB e DESI DR2 BAO em ΛCDM, que se reduz em modelos estendidos.', 'https://arxiv.org/abs/2506.20707'],
    ],
    euclid: [
      ['Q1 público desde 19/mar/2025.', 'https://euclid.caltech.edu/page/data-release-timeline'],
      ['DR1-Foundation previsto para 12/nov/2026 (data provisória): ~1900 deg² de imagens calibradas, catálogos e espectros. Produtos de lenteamento e clustering (LE3) só em meados de 2027.', 'https://www.cosmos.esa.int/web/euclid/timeline'],
    ],
    rubin: [
      ['O LSST começou em 29/jun/2026.', 'https://survey-strategy.lsst.io/progress'],
      ['Alertas são públicos, sem período proprietário, via brokers: ALeRCE, AMPEL, ANTARES, Babamul, Fink, Lasair, Pitt-Google.', 'https://rubinobservatory.org/for-scientists/data-products/alerts-and-brokers'],
      ['Imagens e catálogos (DP1, DP2) só com direito de dados; o período proprietário do DP1 termina em 30/jun/2027.', 'https://www.rubin.community/t/can-the-general-public-get-access-to-the-rubin-data/11864'],
    ],
    des: [
      ['DES Y6 3x2pt (jan/2026) cobre ~5000 deg²; combinado com DESI DR2 BAO, SNe do DES e aglomerados do SPT, difere do CMB em 2,3σ nos parâmetros.', 'https://arxiv.org/abs/2601.14559'],
      ['DES-Dovekie: 1820 SNe com covariâncias; a preferência por energia escura dinâmica caiu de 4,2σ para 3,2σ, descrita pelos autores como fraca (chances de cerca de 5:1).', 'https://arxiv.org/abs/2511.07517'],
    ],
    subaru: [['HSC-SSP PDR3 público desde 31/ago/2021, com cadastro.', 'https://hsc-release.mtk.nao.ac.jp/doc/']],
    jwst: [['Arquivo no MAST; dados públicos após o período proprietário.', 'https://mast.stsci.edu/api/v0/']],
    hubble: [
      ['Orçamento FY2026 sancionado em 23/jan/2026 com US$ 98,3 milhões para o Hubble.', 'https://aas.org/posts/news/2026/01/congress-passes-fiscal-year-2026-spending-bills-nsf-nasa-and-doe'],
      ['Pantheon+SH0ES: 1701 linhas (SNe e hospedeiras com Cefeidas) e covariância completa estatística + sistemática.', 'https://github.com/PantheonPlusSH0ES/DataRelease'],
    ],
    gaia: [['DR4 previsto para 2/dez/2026, com 66 meses de dados; observações científicas encerradas em 15/jan/2025.', 'https://www.cosmos.esa.int/web/gaia/news']],
    planck: [['Missão encerrada; o Planck Legacy Archive guarda todos os produtos públicos.', 'https://www.cosmos.esa.int/web/planck/pla']],
    act: [['O ACT fez a última coleta em 2022 e foi desativado; DR6 público com mapas, espectros, covariâncias e likelihoods.', 'https://act.princeton.edu/']],
    spt: [
      ['SPT-3G D1: likelihood em candl no repositório SouthPoleTelescope/spt_candl_data e mapas públicos via Globus.', 'https://lambda.gsfc.nasa.gov/product/spt/spt3g_d1_bandp_likelihood_info.html'],
      ['O SPT-3G sozinho fica a 6,2σ do SH0ES em H₀.', 'https://arxiv.org/abs/2506.20707'],
    ],
    chime: [
      ['Catálogo 2 de FRBs: 4539 FRBs entre 25/jul/2018 e 15/set/2023; chime-frb.ca/catalog2 está em reconstrução e os dados ficam no registro CANFAR CISTI.CANFAR/25.0066.', 'https://arxiv.org/abs/2601.09399'],
      ['O CHIME detectou o sinal cosmológico de 21 cm em autocorrelação em z ≈ 1; as escolhas da análise removem o sinal de BAO.', 'https://arxiv.org/abs/2511.19620'],
    ],
    meerkat: [
      ['MeerKLASS mediu o autoespectro de 21 cm em escalas cosmológicas: campanha de 2021, ~236 deg², 971,2–1023,6 MHz.', 'https://arxiv.org/abs/2610.12158'],
      ['Levantamento UHF com meta de 10.000 deg² até 2028, cobrindo 0,4 < z < 1,45.', 'https://arxiv.org/abs/2510.27549'],
    ],
    askap: [['WALLABY piloto fase 2: ~1800 fontes HI e modelos cinemáticos de mais de 126 galáxias, na CASDA e no CADC; o levantamento completo tem mais de 8800 h em 5 anos desde nov/2022.', 'https://arxiv.org/abs/2409.13130']],
    erosita: [
      ['DR1 (31/jan/2024): eventos, imagens e catálogos, incluindo os aglomerados eRASS1. DR2 (31/jul/2026): só catálogos, eRASS:3, ~2 milhões de fontes no hemisfério galáctico oeste.', 'https://erosita.mpe.mpg.de/dr2/'],
      ['eROSITA em modo seguro desde 26/fev/2022.', 'https://erosita.mpe.mpg.de/erass/'],
      ['A cosmologia eRASS1 calibrou massas de aglomerados com lenteamento fraco de DES, KiDS e HSC.', 'https://arxiv.org/abs/2402.08458'],
    ],
    chandra: [['Orçamento FY2026: US$ 63 milhões para o Chandra; a proposta do Executivo o cancelava.', 'https://www.planetary.org/articles/advocacy-success-fy2026-nasa-budget']],
    fermi: [['Fótons do LAT públicos tipicamente em um ou dois dias após a aquisição, sem período proprietário.', 'https://fermi.gsfc.nasa.gov/ssc/data/policy']],
  };
  function carregar() {
    for (const e of L) LEDGER.registrar(e);
    for (const [cel, itens] of Object.entries(F)) itens.forEach(([texto, url], i) => LEDGER.registrar({ id: 'F-' + cel + '-' + (i + 1), tag: 'F', celula: cel, texto, fonte: { tipo: 'fato', url } }));
    // constantes do motor, visíveis a todas as células (celula 'rede'), para que citá-las não conte como número inventado
    LEDGER.registrar({ id: 'F-constantes', tag: 'F', celula: 'rede', texto: 'Constantes usadas pelo motor: c = 299792,458 km/s (definição do SI); T_CMB = 2,7255 K (Fixsen 2009); N_eff = 3,044; premissa Σmν = 0,06 eV com ω_ν = 0,0107·Σmν; r_d de Aubourg et al. 2015, eq. 16: 55,154·exp[−72,3(ω_ν+0,0006)²]/(ω_cb^0,25351·ω_b^0,12807) Mpc, precisão 0,021%; S₈ = σ₈·√(Ω_m/0,3).', fonte: { tipo: 'fato', url: 'https://arxiv.org/abs/1411.1074' } });
  }
  return { L, F, carregar };
})();

/* ── 10 · as 17 células ─────────────────────────────────────────────────────
 * Uma por telescópio (levantamento de 16/out/2026). "pipelines" lista só o que
 * foi validado contra resultado publicado; o resto é ingestão + literatura.
 */
const CELULAS = (() => {
  /* Repositórios fixados no commit conferido em 16/out/2026 (reprodutível; para
     atualizar, troque o commit e rode --validar de novo). */
  const REV = {
    bao: 'https://raw.githubusercontent.com/CobayaSampler/bao_data/bb0c1c9009dc76d1391300e169e8df38fd1096db',
    des: 'https://raw.githubusercontent.com/des-science/DES-SN5YR/c9a4fcafc4cbd19bd750dee47fc76194a45c181f',
    pantheon: 'https://raw.githubusercontent.com/PantheonPlusSH0ES/DataRelease/c447f0fea703fcd0fff57de5000947b5ca81286b',
  };
  const P = {
    desi_bao_media: { url: REV.bao + '/desi_bao_dr2/desi_gaussian_bao_ALL_GCcomb_mean.txt', formato: 'texto', descricao: 'DESI DR2 BAO: z, valor, observável (DV/rs, DM/rs, DH/rs), 13 medidas (arXiv:2503.14738)' },
    desi_bao_cov: { url: REV.bao + '/desi_bao_dr2/desi_gaussian_bao_ALL_GCcomb_cov.txt', formato: 'texto', descricao: 'DESI DR2 BAO: covariância 13×13' },
    dovekie_hd: { url: REV.des + '/4_DISTANCES_COVMAT/DES-Dovekie_HD.csv', formato: 'snana', descricao: 'DES-Dovekie: diagrama de Hubble (zHD, zHEL, MU, MUERR) de 1820 SNe Ia' },
    dovekie_cov: { url: REV.des + '/4_DISTANCES_COVMAT/STAT+SYS.npz', formato: 'npz', descricao: 'DES-Dovekie: covariância INVERSA estatística+sistemática, triângulo superior (npz)' },
    dovekie_cov_stat: { url: REV.des + '/4_DISTANCES_COVMAT/STATONLY.npz', formato: 'npz', descricao: 'DES-Dovekie: covariância INVERSA só estatística (npz)' },
    pantheon_dat: { url: REV.pantheon + '/Pantheon+_Data/4_DISTANCES_AND_COVAR/Pantheon+SH0ES.dat', formato: 'espacos', descricao: 'Pantheon+SH0ES: 1701 linhas, m_b_corr, zHD, zHEL, CEPH_DIST, IS_CALIBRATOR' },
    pantheon_cov: { url: REV.pantheon + '/Pantheon+_Data/4_DISTANCES_AND_COVAR/Pantheon+SH0ES_STAT+SYS.cov', formato: 'covPantheon', descricao: 'Pantheon+SH0ES: covariância 1701×1701 estatística+sistemática (33 MB)' },
    erosita_dr1_catalogos: { url: 'https://erosita.mpe.mpg.de/dr1/AllSkySurveyData_dr1/Catalogues_dr1/', formato: 'indice', descricao: 'eROSITA-DE DR1: diretório de catálogos (inclui aglomerados eRASS1), FITS' },
  };
  const lista = [
    { id: 'desi', n: 1, nome: 'DESI', telescopio: 'Mayall 4 m, Kitt Peak (EUA)', banda: 'óptico', sondas: 'BAO de galáxias, quasares e floresta Lyα; RSD', produtos: ['desi_bao_media', 'desi_bao_cov'], tap: ['datalab'], pipelines: ['bao_desi'], status: 'validado' },
    { id: 'euclid', n: 2, nome: 'Euclid', telescopio: 'ESA, espaço (L2)', banda: 'óptico/IV', sondas: 'lenteamento fraco, clustering espectroscópico, lentes fortes', produtos: [], tap: ['irsa'], pipelines: [], status: 'parcial' },
    { id: 'rubin', n: 3, nome: 'Rubin', telescopio: 'Simonyi 8,4 m, Cerro Pachón (Chile)', banda: 'óptico', sondas: 'SNe Ia, lenteamento fraco, lentes fortes, satélites da Via Láctea', produtos: [], tap: [], pipelines: [], status: 'parcial' },
    { id: 'des', n: 4, nome: 'Blanco/DECam (DES)', telescopio: 'Blanco 4 m, Cerro Tololo (Chile)', banda: 'óptico', sondas: '3x2pt, SNe Ia, BAO, aglomerados', produtos: ['dovekie_hd', 'dovekie_cov', 'dovekie_cov_stat'], tap: ['datalab'], pipelines: ['sn_dovekie'], status: 'validado' },
    { id: 'subaru', n: 5, nome: 'Subaru (HSC)', telescopio: 'Subaru 8,2 m, Maunakea (Havaí)', banda: 'óptico', sondas: 'lenteamento fraco', produtos: [], tap: [], pipelines: [], status: 'literatura' },
    { id: 'jwst', n: 6, nome: 'JWST', telescopio: 'espaço (L2)', banda: 'infravermelho', sondas: 'escada de distâncias, lentes fortes, subestrutura de MD', produtos: [], tap: [], pipelines: [], status: 'literatura' },
    { id: 'hubble', n: 7, nome: 'Hubble', telescopio: 'espaço (órbita baixa)', banda: 'óptico/UV', sondas: 'escada de distâncias (SH0ES), lenteamento de aglomerados', produtos: ['pantheon_dat', 'pantheon_cov'], tap: [], pipelines: ['shoes_h0'], status: 'validado' },
    { id: 'gaia', n: 8, nome: 'Gaia', telescopio: 'ESA, espaço (L2)', banda: 'astrometria', sondas: 'halo de MD da Via Láctea; paralaxes da escada', produtos: [], tap: ['gaia', 'simbad'], pipelines: [], status: 'ingestão' },
    { id: 'planck', n: 9, nome: 'Planck', telescopio: 'ESA, espaço (encerrado)', banda: 'micro-ondas', sondas: 'CMB T/E e lenteamento', produtos: [], tap: [], pipelines: [], status: 'literatura' },
    { id: 'act', n: 10, nome: 'ACT', telescopio: '6 m, Atacama (Chile, desativado)', banda: 'micro-ondas', sondas: 'CMB em alta resolução, lenteamento, SZ', produtos: [], tap: [], pipelines: [], status: 'literatura' },
    { id: 'spt', n: 11, nome: 'SPT', telescopio: '10 m, Polo Sul', banda: 'micro-ondas', sondas: 'CMB, lenteamento, aglomerados SZ', produtos: [], tap: [], pipelines: [], status: 'literatura' },
    { id: 'chime', n: 12, nome: 'CHIME', telescopio: 'DRAO, Canadá (400–800 MHz)', banda: 'rádio', sondas: '21 cm em intensidade; FRBs', produtos: [], tap: [], pipelines: [], status: 'literatura' },
    { id: 'meerkat', n: 13, nome: 'MeerKAT', telescopio: '64 antenas, Karoo (África do Sul)', banda: 'rádio', sondas: '21 cm em intensidade (MeerKLASS); HI de galáxias', produtos: [], tap: [], pipelines: [], status: 'literatura' },
    { id: 'askap', n: 14, nome: 'ASKAP', telescopio: 'Murchison (Austrália)', banda: 'rádio', sondas: 'HI de galáxias (WALLABY); FRBs localizados', produtos: [], tap: ['casda'], pipelines: [], status: 'ingestão' },
    { id: 'erosita', n: 15, nome: 'eROSITA', telescopio: 'SRG, espaço', banda: 'raios X', sondas: 'aglomerados de galáxias', produtos: ['erosita_dr1_catalogos'], tap: ['vizier'], pipelines: [], status: 'ingestão' },
    { id: 'chandra', n: 16, nome: 'Chandra', telescopio: 'espaço', banda: 'raios X', sondas: 'massa e fração de gás de aglomerados; separação gás–massa', produtos: [], tap: ['heasarc'], pipelines: [], status: 'ingestão' },
    { id: 'fermi', n: 17, nome: 'Fermi-LAT', telescopio: 'espaço', banda: 'raios gama', sondas: 'aniquilação de MD em anãs e no centro galáctico', produtos: [], tap: ['heasarc'], pipelines: [], status: 'ingestão' },
  ];
  const porId = Object.fromEntries(lista.map((c) => [c.id, c]));
  /* Cadeias oficiais usadas por --validar (alvos calculados na hora, nada embutido). */
  const CADEIAS = {
    bao_lcdm: { url: REV.des + '/5_COSMOLOGY/chains/flcdm/bao_lcdm_nautilus.txt', amostrador: 'nautilus' },
    sn_lcdm: { url: REV.des + '/5_COSMOLOGY/chains/flcdm/dovekie_lcdm_nautilus.txt', amostrador: 'nautilus' },
    bao_sn_wcdm: { url: REV.des + '/5_COSMOLOGY/chains/wlcdm/dovekie_bao_wcdm_nautilus.txt', amostrador: 'nautilus' },
    bao_sn_w0wacdm: { url: REV.des + '/5_COSMOLOGY/chains/w0wacdm/dovekie_bao_w0wacdm_nautilus.txt', amostrador: 'nautilus' },
    shoes_lcdm: { url: REV.pantheon + '/Pantheon+_Data/5_COSMOLOGY/chains/Pantheon+SH0ES/Pantheon+SH0ES_FlatLambdaCDM.txt.gz', amostrador: 'emcee' },
  };
  /* Domínios de onde uma célula pode baixar arquivos listados num índice. */
  const DOMINIOS_INDICE = { erosita_dr1_catalogos: 'https://erosita.mpe.mpg.de/dr1/' };
  return { PRODUTOS: P, lista, porId, DOMINIOS_INDICE, REV, CADEIAS };
})();

/* ── 11 · pipelines validados ──────────────────────────────────────────────
 *   bao_desi   DESI DR2 BAO (13 medidas + covariância)        → Ω_m, h·r_d, w…
 *   sn_dovekie DES-Dovekie (1820 SNe, covariância inversa)      → Ω_m, w…
 *   shoes_h0   Pantheon+SH0ES (Cefeidas calibram M)              → H₀, Ω_m, M
 *   conjunto   soma de χ² de verossimilhanças independentes      → rede
 * Cada pipeline reproduz a verossimilhança oficial distribuída com os dados
 * (Cobaya/CosmoSIS) e foi conferido contra as cadeias oficiais (--validar).
 */
const PIPE = (() => {
  const cache = {};
  const prod = CELULAS.PRODUTOS;

  function memo(chave, fn) {
    if (!cache[chave]) cache[chave] = fn().catch((e) => { delete cache[chave]; throw e; });
    return cache[chave];
  }
  const dadosBAO = () => memo('bao', async () => {
    const m = await DADOS.baixar(prod.desi_bao_media.url), c = await DADOS.baixar(prod.desi_bao_cov.url);
    const pontos = DADOS.texto(m.buf).split(/\r?\n/).map((l) => l.trim()).filter((l) => l && !l.startsWith('#'))
      .map((l) => { const [z, v, q] = l.split(/\s+/); return { z: Number(z), v: Number(v), q }; });
    const toks = DADOS.texto(c.buf).trim().split(/\s+/).map(Number);
    const n = pontos.length;
    if (toks.length !== n * n) throw new Error('BAO: covariância com ' + toks.length + ' valores para ' + n + ' medidas');
    const C = Float64Array.from(toks);
    for (let i = 0; i < n; i++) for (let j = 0; j < i; j++) if (Math.abs(C[i * n + j] - C[j * n + i]) > 1e-12 * (Math.abs(C[i * n + j]) + 1e-30)) throw new Error('BAO: covariância não simétrica');
    return { pontos, n, C, L: ALG.cholesky(C, n), zmax: Math.max(...pontos.map((p) => p.z)), arquivos: [m, c] };
  });
  function chi2BAO(d, p, hrd, dz) {
    if (!(hrd > 0)) return Infinity;
    const t = COSMO.tabela(p, d.zmax + 0.02, dz);
    if (!t) return Infinity;
    const r = new Float64Array(d.n);
    for (let i = 0; i < d.n; i++) { const pt = d.pontos[i]; r[i] = COSMO.baoRazao(pt.q, pt.z, t, hrd) - pt.v; }
    const y = ALG.resolverInferior(d.L, d.n, r);
    return ALG.dot(y, y);
  }

  const dadosDovekie = (tipoCov = 'STAT+SYS') => memo('dovekie_' + tipoCov, async () => {
    const h = await DADOS.baixar(prod.dovekie_hd.url);
    const c = await DADOS.baixar(tipoCov === 'STATONLY' ? prod.dovekie_cov_stat.url : prod.dovekie_cov.url);
    const hd = DADOS.snana(DADOS.texto(h.buf));
    const col = (nome) => { const i = hd.colunas.indexOf(nome); if (i < 0) throw new Error('Dovekie: coluna ' + nome + ' ausente'); return hd.linhas.map((l) => l[i]); };
    const zHD = col('zHD'), zHEL = col('zHEL'), MU = col('MU');
    const arr = DADOS.npz(c.buf);
    const n = Number(arr.nsn.dados[0]);
    const tri = arr.cov.dados;
    if (tri.length !== n * (n + 1) / 2) throw new Error('Dovekie: triângulo com ' + tri.length + ' valores para n=' + n);
    if (zHD.length !== n) throw new Error('Dovekie: ' + zHD.length + ' SNe no HD e ' + n + ' na covariância');
    if (zHD.some((z) => !(z > 0))) throw new Error('Dovekie: zHD não positivo (o filtro oficial é zHD > 0)');
    const W = new Float64Array(n * n);
    let k = 0;
    for (let i = 0; i < n; i++) for (let j = i; j < n; j++) { W[i * n + j] = tri[k]; W[j * n + i] = tri[k]; k++; }
    const um = new Float64Array(n).fill(1);
    const W1 = ALG.matVec(W, n, um);
    const C1 = W1.reduce((s, v) => s + v, 0);
    return { n, zHD: Float64Array.from(zHD), zHEL: Float64Array.from(zHEL), MU: Float64Array.from(MU), W, W1, C1, zmax: Math.max(...zHD), arquivos: [h, c], tipoCov };
  });
  /* χ² com M marginalizado analiticamente (Goliath et al. 2001, eq. A9–A12),
     igual à verossimilhança oficial DES-Dovekie, sem a constante ln(C/2π). */
  function chi2SN(d, p, dz) {
    const t = COSMO.tabela(p, d.zmax + 0.02, dz);
    if (!t) return Infinity;
    const D = new Float64Array(d.n);
    for (let i = 0; i < d.n; i++) D[i] = COSMO.modulo(d.zHD[i], d.zHEL[i], t, 70) - d.MU[i];
    const WD = ALG.matVec(d.W, d.n, D);
    const chit2 = ALG.dot(D, WD), B = ALG.dot(d.W1, D);
    return chit2 - (B * B) / d.C1;
  }

  const dadosPantheon = () => memo('pantheon', async () => {
    const a = await DADOS.baixar(prod.pantheon_dat.url), c = await DADOS.baixar(prod.pantheon_cov.url);
    const tab = DADOS.espacos(DADOS.texto(a.buf));
    const col = (nome) => { const i = tab.colunas.indexOf(nome); if (i < 0) throw new Error('Pantheon+: coluna ' + nome + ' ausente'); return tab.linhas.map((l) => l[i]); };
    const zHD = col('zHD'), zHEL = col('zHEL'), mb = col('m_b_corr'), cal = col('IS_CALIBRATOR'), ceph = col('CEPH_DIST');
    const cov = DADOS.covPantheon(DADOS.texto(c.buf));
    if (cov.n !== zHD.length) throw new Error('Pantheon+: ' + zHD.length + ' linhas e covariância ' + cov.n);
    // filtro oficial: zHD > 0,01 ou calibrador
    const idx = [];
    for (let i = 0; i < zHD.length; i++) if (zHD[i] > 0.01 || cal[i] === 1) idx.push(i);
    const n = idx.length, N = cov.n;
    const C = new Float64Array(n * n);
    for (let a2 = 0; a2 < n; a2++) for (let b2 = 0; b2 < n; b2++) C[a2 * n + b2] = cov.C[idx[a2] * N + idx[b2]];
    const L = ALG.cholesky(C, n);
    const u = ALG.resolverInferior(L, n, new Float64Array(n).fill(1));
    const d = {
      n, L, u, uu: ALG.dot(u, u), zHD: Float64Array.from(idx.map((i) => zHD[i])), zHEL: Float64Array.from(idx.map((i) => zHEL[i])),
      mb: Float64Array.from(idx.map((i) => mb[i])), cal: idx.map((i) => cal[i] === 1), ceph: Float64Array.from(idx.map((i) => ceph[i])),
      nCal: idx.filter((i) => cal[i] === 1).length, arquivos: [a, c],
    };
    d.zmax = Math.max(...d.zHD);
    return d;
  });
  /* χ² com M minimizado analiticamente (parâmetro linear: perfil = marginal). */
  function chi2SH0ES(d, p, H0, dz, devolverM) {
    if (!(H0 > 0)) return Infinity;
    const t = COSMO.tabela(p, d.zmax + 0.02, dz);
    if (!t) return Infinity;
    const r = new Float64Array(d.n);
    for (let i = 0; i < d.n; i++) r[i] = d.mb[i] - (d.cal[i] ? d.ceph[i] : COSMO.modulo(d.zHD[i], d.zHEL[i], t, H0));
    const v = ALG.resolverInferior(d.L, d.n, r);
    const uv = ALG.dot(d.u, v);
    const chi2 = ALG.dot(v, v) - (uv * uv) / d.uu;
    return devolverM ? { chi2, M: uv / d.uu } : chi2;
  }

  /* ── modelos e ajuste ─────────────────────────────────────────────────── */
  /* Caixas de prior de w e wa iguais às das cadeias oficiais DES-SN5YR
     (values.ini no cabeçalho das cadeias): w ∈ [−3; −0,4], wa ∈ [−3; 2],
     w0 + wa < 0. Ω_m fica numa caixa larga (0,01–0,99). */
  const MODELOS = {
    lcdm: { rotulo: 'ΛCDM plano', nomes: ['Om'], x0: [0.3], passos: [0.05], dentro: (x) => x[0] > 0.01 && x[0] < 0.99, cosmo: (x) => ({ Om: x[0] }) },
    wcdm: { rotulo: 'wCDM plano', nomes: ['Om', 'w'], x0: [0.3, -1], passos: [0.05, 0.2], dentro: (x) => x[0] > 0.01 && x[0] < 0.99 && x[1] > -3 && x[1] < -0.4, cosmo: (x) => ({ Om: x[0], w0: x[1], wa: 0 }) },
    w0wacdm: { rotulo: 'w0waCDM plano', nomes: ['Om', 'w0', 'wa'], x0: [0.3, -0.9, -0.3], passos: [0.05, 0.2, 0.5], dentro: (x) => x[0] > 0.01 && x[0] < 0.99 && x[1] > -3 && x[1] < -0.4 && x[2] > -3 && x[2] < 2 && x[1] + x[2] < 0, cosmo: (x) => ({ Om: x[0], w0: x[1], wa: x[2] }) },
  };
  /* Monta a função χ² total para um conjunto de verossimilhanças. */
  function montar(componentes, modelo, opcoes) {
    const M = MODELOS[modelo];
    if (!M) throw new Error('modelo desconhecido: ' + modelo + ' (use lcdm, wcdm ou w0wacdm)');
    const nomes = [...M.nomes], x0 = [...M.x0], passos = [...M.passos];
    const usaBAO = componentes.includes('bao'), usaSN = componentes.includes('sn'), usaSH0ES = componentes.includes('shoes');
    if (usaBAO) { nomes.push('hrd'); x0.push(opcoes.variante ? 98 : 101); passos.push(3); }
    if (usaSH0ES) { nomes.push('H0'); x0.push(opcoes.variante ? 72 : 73); passos.push(1.5); }
    const iH = nomes.indexOf('H0'), iR = nomes.indexOf('hrd');
    const dz = opcoes.variante ? 0.0025 : 0.005;
    const dentro = (x) => M.dentro(x) && (iR < 0 || (x[iR] > 50 && x[iR] < 200)) && (iH < 0 || (x[iH] > 40 && x[iH] < 100));
    const chi2 = (x) => {
      if (!dentro(x)) return Infinity;
      const p = M.cosmo(x);
      if (opcoes.radiacao) { const h = iH >= 0 ? x[iH] / 100 : opcoes.hRadiacao; p.Or = COSMO.OrDe(h); }
      let s = 0;
      if (usaBAO) s += chi2BAO(opcoes.d.bao, p, x[iR], dz);
      if (usaSN) s += chi2SN(opcoes.d.sn, p, dz);
      if (usaSH0ES) s += chi2SH0ES(opcoes.d.shoes, p, x[iH], dz);
      return Number.isFinite(s) ? s : Infinity;
    };
    return { nomes, x0, passos, chi2, dentro, M, dz };
  }
  async function carregarComponentes(componentes, opcoes) {
    const d = {};
    if (componentes.includes('bao')) d.bao = await dadosBAO();
    if (componentes.includes('sn')) d.sn = await dadosDovekie(opcoes.covSN || 'STAT+SYS');
    if (componentes.includes('shoes')) d.shoes = await dadosPantheon();
    return d;
  }
  /* Ajuste: ΛCDM por mínimo + Laplace; modelos estendidos por MCMC. */
  async function ajustar(componentes, modelo = 'lcdm', opcoes = {}) {
    const op = { radiacao: opcoes.radiacao !== false, hRadiacao: opcoes.hRadiacao || 0.7, variante: !!opcoes.variante, covSN: opcoes.covSN };
    op.d = await carregarComponentes(componentes, op);
    const m = montar(componentes, modelo, op);
    const t0 = Date.now();
    const r = ALG.minimizar(m.chi2, m.x0, m.passos, { maxIter: 3000, tolAbs: 1e-8, reinicios: 3 });
    if (!Number.isFinite(r.f)) throw new Error('ajuste não convergiu (χ² infinito)');
    let n = 0;
    if (op.d.bao) n += op.d.bao.n;
    if (op.d.sn) n += op.d.sn.n;
    if (op.d.shoes) n += op.d.shoes.n;
    const nLivres = m.nomes.length + (op.d.sn ? 1 : 0) + (op.d.shoes ? 1 : 0);
    const res = { componentes, modelo, rotuloModelo: m.M.rotulo, nomes: m.nomes, melhor: r.x, chi2min: r.f, n, ndof: n - nLivres, parametros: {}, configuracao: { radiacao: op.radiacao, hRadiacao: op.hRadiacao, dz: m.dz, variante: op.variante, covSN: op.covSN || (op.d.sn ? 'STAT+SYS' : undefined) } };
    const escala = m.passos.map((p) => p / 10);
    let cov = null;
    try { cov = ALG.covarianciaLaplace(m.chi2, r.x, escala); } catch (e) { cov = null; }
    if (modelo === 'lcdm' && cov) {
      res.metodo = 'mínimo por Nelder–Mead + covariância de Laplace (Hessiana numérica)';
      m.nomes.forEach((nome, i) => { res.parametros[nome] = { valor: r.x[i], sigma: Math.sqrt(cov[i][i]) }; });
      res.correlacao = cov.map((lin, i) => lin.map((v, j) => v / Math.sqrt(cov[i][i] * cov[j][j])));
    } else {
      const propCov = cov || m.passos.map((p, i) => m.passos.map((q, j) => (i === j ? (p / 5) * (p / 5) : 0)));
      const passos = opcoes.mcmcPassos || (op.d.sn || op.d.shoes ? Math.min(CFG.MCMC_PASSOS, 8000) : CFG.MCMC_PASSOS);
      const mc = await ALG.mcmc((x) => -0.5 * m.chi2(x), r.x, propCov, { passos, semente: opcoes.semente || 20261016, sinal: opcoes.sinal });
      res.metodo = 'Metropolis (' + passos + ' passos após aquecimento, priors planos na caixa do modelo); valor = mediana, barras = percentis 16–84';
      res.aceitacao = mc.aceitacao;
      m.nomes.forEach((nome, i) => {
        const s = mc.resumo[i];
        res.parametros[nome] = { valor: s.mediana, sigma_mais: s.p84 - s.mediana, sigma_menos: s.mediana - s.p16, media: s.media, desvio: s.desvio };
      });
    }
    if (op.d.shoes) {
      const p = m.M.cosmo(r.x);
      if (op.radiacao) p.Or = COSMO.OrDe(r.x[m.nomes.indexOf('H0')] / 100);
      const mm = chi2SH0ES(op.d.shoes, p, r.x[m.nomes.indexOf('H0')], m.dz, true);
      res.M_melhor = mm.M;
    }
    res.ms = Date.now() - t0;
    res.arquivos = Object.values(op.d).flatMap((x) => x.arquivos.map((a) => ({ url: a.url, sha256: a.sha256, bytes: a.bytes })));
    res.p = res.ndof > 0 ? ALG.pQuiQuadrado(res.chi2min, res.ndof) : null;
    return res;
  }

  const DADOS_USADOS = { bao: ['desi'], sn: ['des-sn', 'cfa', 'foundation'], shoes: ['pantheon+', 'shoes-cefeidas', 'cfa', 'foundation', 'des-sn-3yr', 'gaia'] };
  const NOME_COMP = { bao: 'DESI DR2 BAO', sn: 'DES-Dovekie', shoes: 'Pantheon+SH0ES' };
  /* Registra os parâmetros publicáveis no ledger como [C]. */
  function registrar(celula, res, entradasD) {
    const variante = [res.configuracao.covSN === 'STATONLY' ? 'covariância só estatística' : '', res.configuracao.radiacao === false ? 'sem radiação' : ''].filter(Boolean);
    const fonte = res.componentes.map((c) => NOME_COMP[c]).join(' + ') + (variante.length ? ' [checagem: ' + variante.join(', ') + ']' : '');
    const dados = [...new Set(res.componentes.flatMap((c) => DADOS_USADOS[c]))];
    const regs = [];
    for (const [nome, v] of Object.entries(res.parametros)) {
      if (!LEDGER.GRANDEZAS_PUBLICAVEIS[nome]) continue;
      regs.push(LEDGER.registrar({
        tag: 'C', celula, grandeza: nome, valor: v.valor, sigma: v.sigma, sigma_mais: v.sigma_mais, sigma_menos: v.sigma_menos,
        unidade: LEDGER.GRANDEZAS_PUBLICAVEIS[nome].unidade, modelo: res.rotuloModelo,
        rotulo: LEDGER.GRANDEZAS_PUBLICAVEIS[nome].rotulo + ' — ' + fonte + ', ' + res.rotuloModelo,
        dados_usados: dados, fonte: { tipo: 'calculo', descricao: fonte + ' (' + res.metodo + ')' }, metodo: res.metodo,
        entradas: entradasD || [], chi2: res.chi2min, n: res.n, ndof: res.ndof, p: res.p ?? undefined,
        numeros: [res.chi2min, res.chi2min / Math.max(1, res.ndof), ...(res.aceitacao ? [res.aceitacao] : []), ...(res.M_melhor !== undefined ? [res.M_melhor] : []), v.media, v.desvio].filter((x) => Number.isFinite(x)),
        configuracao: res.configuracao, arquivos: res.arquivos,
        refazer: { componentes: res.componentes, modelo: res.modelo, opcoes: { radiacao: res.configuracao.radiacao, covSN: res.configuracao.covSN } },
      }));
    }
    if (res.M_melhor !== undefined) {
      regs.push(LEDGER.registrar({ tag: 'C', celula, grandeza: 'M', valor: res.M_melhor, unidade: 'mag', modelo: res.rotuloModelo,
        rotulo: 'M (magnitude absoluta SN Ia no melhor ajuste) — ' + fonte, dados_usados: dados, fonte: { tipo: 'calculo', descricao: 'mínimo analítico em M' }, metodo: 'mínimo analítico (parâmetro linear)', entradas: entradasD || [] }));
    }
    return regs;
  }
  /* Estatística descritiva de uma tabela já registrada como [D]. */
  function estatistica(tabela, colunas) {
    const usar = (colunas && colunas.length ? colunas : tabela.colunas).slice(0, 25);
    const out = {};
    for (const c of usar) {
      const i = tabela.colunas.indexOf(c);
      if (i < 0) continue;
      const vals = tabela.linhas.map((l) => l[i]).filter((v) => typeof v === 'number');
      if (vals.length) out[c] = ALG.descrever(vals);
    }
    return out;
  }
  return { dadosBAO, dadosDovekie, dadosPantheon, chi2BAO, chi2SN, chi2SH0ES, ajustar, registrar, estatistica, MODELOS, DADOS_USADOS, NOME_COMP, cache };
})();

/* ── 12 · Polo V: verificação determinística ───────────────────────────────
 * Identidades, tensões, combinações e as pontes entre células (régua BAO ×
 * CMB × escada). Toda saída vira registro [C] com entradas rastreáveis.
 */
const POLOV = (() => {
  const SIGMA_MNU_PREMISSA = 0.06; // eV: premissa de soma mínima (ordenamento normal), declarada em cada registro
  const sobrepoe = (a, b) => (a.dados_usados || []).filter((x) => (b.dados_usados || []).includes(x));

  function tensao(idA, idB, celula = 'rede') {
    const a = LEDGER.obter(idA), b = LEDGER.obter(idB);
    if (!a || !b) throw new Error('registro inexistente: ' + (!a ? idA : idB));
    if (a.grandeza !== b.grandeza) throw new Error('grandezas diferentes: ' + a.grandeza + ' × ' + b.grandeza);
    const sa = LEDGER.sigmaLado(a, b.valor), sb = LEDGER.sigmaLado(b, a.valor);
    if (!(sa > 0) || !(sb > 0)) throw new Error('registro sem incerteza');
    const n = Math.abs(a.valor - b.valor) / Math.sqrt(sa * sa + sb * sb);
    const comum = sobrepoe(a, b);
    const avisos = [];
    if (comum.length) avisos.push('não independentes: compartilham ' + comum.join(', '));
    if (a.modelo && b.modelo && a.modelo !== b.modelo) avisos.push('modelos diferentes: ' + a.modelo + ' × ' + b.modelo);
    return LEDGER.registrar({
      tag: 'C', celula, grandeza: 'tensao', valor: n, unidade: 'σ', rotulo: 'tensão em ' + (LEDGER.GRANDEZAS_PUBLICAVEIS[a.grandeza] || {}).rotulo + ': ' + a.id + ' × ' + b.id,
      entradas: [a.id, b.id], metodo: '|a−b| / √(σa²+σb²), com σ do lado voltado para o outro valor', avisos, numeros: [a.valor - b.valor, sa, sb],
      fonte: { tipo: 'calculo', descricao: 'Polo V · tensão' }, p: ALG.erfc(n / Math.SQRT2),
    });
  }

  /* Média ponderada só de registros independentes e do mesmo modelo. */
  function combinar(ids, celula = 'rede') {
    const es = ids.map((id) => { const e = LEDGER.obter(id); if (!e) throw new Error('registro inexistente: ' + id); return e; });
    const g = es[0].grandeza;
    if (es.some((e) => e.grandeza !== g)) throw new Error('combinação exige a mesma grandeza');
    for (let i = 0; i < es.length; i++) for (let j = i + 1; j < es.length; j++) {
      const c = sobrepoe(es[i], es[j]);
      if (c.length) throw new Error('recusado: ' + es[i].id + ' e ' + es[j].id + ' não são independentes (compartilham ' + c.join(', ') + ')');
    }
    const modelos = [...new Set(es.map((e) => e.modelo))];
    let sw = 0, swx = 0;
    const ws = es.map((e) => { const s = LEDGER.sigmaSimetrico(e); const w = 1 / (s * s); sw += w; swx += w * e.valor; return w; });
    const media = swx / sw, sigma = 1 / Math.sqrt(sw);
    const chi2 = es.reduce((s, e, i) => s + ws[i] * (e.valor - media) * (e.valor - media), 0);
    const ndof = es.length - 1, p = ndof > 0 ? ALG.pQuiQuadrado(chi2, ndof) : 1;
    return LEDGER.registrar({
      tag: 'C', celula, grandeza: g, valor: media, sigma, unidade: es[0].unidade, modelo: modelos.length === 1 ? modelos[0] : 'misto: ' + modelos.join(' / '),
      rotulo: (LEDGER.GRANDEZAS_PUBLICAVEIS[g] || {}).rotulo + ' combinado (' + es.length + ' medidas)', entradas: es.map((e) => e.id),
      dados_usados: [...new Set(es.flatMap((e) => e.dados_usados || []))], chi2, ndof, p,
      numeros: [Math.sqrt(chi2 / Math.max(1, ndof)), ALG.sigmaDeP(p)], avisos: [...(modelos.length > 1 ? ['modelos diferentes'] : []), ...(p < 0.0027 ? ['medidas inconsistentes entre si (p < 0,0027): a média não representa nenhuma delas'] : [])],
      metodo: 'média ponderada pelo inverso da variância; σ simetrizado; χ² de consistência', fonte: { tipo: 'calculo', descricao: 'Polo V · combinação' },
    });
  }

  /* r_d pelo CMB: Aubourg et al. 2015 com ω_b e ω_c de um mesmo registro-fonte. */
  function rdCMB(idOmb, idOmc, celula = 'rede') {
    const b = LEDGER.obter(idOmb), c = LEDGER.obter(idOmc);
    if (!b || !c || b.grandeza !== 'ombh2' || c.grandeza !== 'omch2') throw new Error('rdCMB exige registros ombh2 e omch2');
    const omcb = b.valor + c.valor, rd = COSMO.rdAubourg(b.valor, omcb, SIGMA_MNU_PREMISSA);
    const sOmcb = Math.sqrt(LEDGER.sigmaSimetrico(b) ** 2 + LEDGER.sigmaSimetrico(c) ** 2);
    const rel = Math.sqrt((0.12807 * LEDGER.sigmaSimetrico(b) / b.valor) ** 2 + (0.25351 * sOmcb / omcb) ** 2);
    return LEDGER.registrar({
      tag: 'C', celula, grandeza: 'rd', valor: rd, sigma: rd * rel, unidade: 'Mpc', modelo: b.modelo,
      rotulo: 'r_d pelo CMB (Aubourg 2015) com ω_b e ω_c de ' + (b.fonte.descricao || b.id), entradas: [b.id, c.id],
      dados_usados: [...new Set([...(b.dados_usados || []), ...(c.dados_usados || [])])],
      metodo: 'r_d ≈ 55,154·exp[−72,3(ω_ν+0,0006)²]/(ω_cb^0,25351·ω_b^0,12807) Mpc; σ linear ignorando a correlação ω_b–ω_c',
      premissas: ['Σmν = ' + SIGMA_MNU_PREMISSA + ' eV (ω_ν = 0,0107·Σmν)'], numeros: [omcb, sOmcb, SIGMA_MNU_PREMISSA], fonte: { tipo: 'calculo', descricao: 'Polo V · r_d (CMB)' },
    });
  }
  /* r_d tardio: h·r_d do BAO dividido por h de uma medida local de H0. */
  function rdTardio(idHrd, idH0, celula = 'rede') {
    const a = LEDGER.obter(idHrd), b = LEDGER.obter(idH0);
    if (!a || !b || a.grandeza !== 'hrd' || b.grandeza !== 'H0') throw new Error('rdTardio exige hrd e H0');
    const comum = sobrepoe(a, b);
    if (comum.length) throw new Error('recusado: hrd e H0 compartilham ' + comum.join(', '));
    const h = b.valor / 100, rd = a.valor / h;
    const rel = Math.sqrt((LEDGER.sigmaSimetrico(a) / a.valor) ** 2 + (LEDGER.sigmaSimetrico(b) / b.valor) ** 2);
    return LEDGER.registrar({ tag: 'C', celula, grandeza: 'rd', valor: rd, sigma: rd * rel, unidade: 'Mpc', modelo: a.modelo, rotulo: 'r_d tardio = h·r_d (' + a.id + ') / h (' + b.id + ')', entradas: [a.id, b.id], dados_usados: [...new Set([...(a.dados_usados || []), ...(b.dados_usados || [])])], metodo: 'razão; σ por propagação linear de medidas independentes', fonte: { tipo: 'calculo', descricao: 'Polo V · r_d tardio' } });
  }
  /* H0 pela régua inversa: h·r_d do BAO dividido por r_d do CMB. */
  function h0ReguaInversa(idHrd, idRd, celula = 'rede') {
    const a = LEDGER.obter(idHrd), b = LEDGER.obter(idRd);
    if (!a || !b || a.grandeza !== 'hrd' || b.grandeza !== 'rd') throw new Error('h0ReguaInversa exige hrd e rd');
    const comum = sobrepoe(a, b);
    if (comum.length) throw new Error('recusado: hrd e r_d compartilham ' + comum.join(', '));
    const H0 = 100 * a.valor / b.valor;
    const rel = Math.sqrt((LEDGER.sigmaSimetrico(a) / a.valor) ** 2 + (LEDGER.sigmaSimetrico(b) / b.valor) ** 2);
    return LEDGER.registrar({ tag: 'C', celula, grandeza: 'H0', valor: H0, sigma: H0 * rel, unidade: 'km/s/Mpc', modelo: a.modelo, rotulo: 'H₀ pela régua inversa = 100·h·r_d (' + a.id + ') / r_d (' + b.id + ')', entradas: [a.id, b.id], dados_usados: [...new Set([...(a.dados_usados || []), ...(b.dados_usados || [])])], metodo: 'razão; σ por propagação linear', premissas: b.premissas || [], fonte: { tipo: 'calculo', descricao: 'Polo V · régua inversa' } });
  }
  /* Identidades de um mesmo registro-fonte (mesmo artigo e modelo). */
  function identidades(celula = 'rede') {
    const lit = LEDGER.listar({ tag: 'D' }).filter((e) => e.fonte && e.fonte.tipo === 'literatura' && !e.copia_de);
    const grupos = {};
    for (const e of lit) (grupos[e.fonte.descricao] = grupos[e.fonte.descricao] || {})[e.grandeza] = e;
    const out = [];
    for (const [fonte, g] of Object.entries(grupos)) {
      if (g.ombh2 && g.omch2 && g.H0 && g.Om) {
        const h = g.H0.valor / 100, omnu = 0.0107 * SIGMA_MNU_PREMISSA;
        const calc = (g.ombh2.valor + g.omch2.valor + omnu) / (h * h);
        const n = Math.abs(calc - g.Om.valor) / LEDGER.sigmaSimetrico(g.Om);
        out.push(LEDGER.registrar({ tag: 'C', celula, grandeza: null, valor: calc, unidade: '', rotulo: 'Ω_m = (ω_b+ω_c+ω_ν)/h² — ' + fonte, entradas: [g.ombh2.id, g.omch2.id, g.H0.id, g.Om.id], numeros: [n, omnu], premissas: ['Σmν = ' + SIGMA_MNU_PREMISSA + ' eV'], metodo: 'identidade; diferença em unidades de σ(Ω_m) do próprio registro', ok: n < 1, fonte: { tipo: 'calculo', descricao: 'Polo V · identidade' } }));
      }
      if (g.sigma8 && g.Om) {
        const s8 = COSMO.S8de(g.sigma8.valor, g.Om.valor);
        out.push(LEDGER.registrar({ tag: 'C', celula, grandeza: null, valor: s8, rotulo: 'S₈ = σ₈√(Ω_m/0,3) (valor central; σ exige a covariância σ₈–Ω_m, não publicada no resumo) — ' + fonte, entradas: [g.sigma8.id, g.Om.id], metodo: 'identidade', fonte: { tipo: 'calculo', descricao: 'Polo V · identidade' } }));
      }
      if (g.H0 && g.Om && (g.Om.modelo || '').includes('ΛCDM')) {
        const t0 = COSMO.idadeLCDM(g.H0.valor, g.Om.valor);
        out.push(LEDGER.registrar({ tag: 'C', celula, grandeza: null, valor: t0, unidade: 'Gyr', rotulo: 'idade t₀ (ΛCDM plano, sem radiação) — ' + fonte, entradas: [g.H0.id, g.Om.id], metodo: 't₀ = 2/(3H₀√Ω_Λ)·asinh√(Ω_Λ/Ω_m)', fonte: { tipo: 'calculo', descricao: 'Polo V · identidade' } }));
      }
    }
    return out;
  }
  /* Tabela da rede por grandeza: medidas publicadas e tensões par a par. */
  function tabelaRede() {
    const pub = LEDGER.listar({ publicado: true });
    const porG = {};
    for (const e of pub) (porG[e.grandeza] = porG[e.grandeza] || []).push(e);
    const out = {};
    for (const [g, es] of Object.entries(porG)) {
      const pares = [];
      for (let i = 0; i < es.length; i++) for (let j = i + 1; j < es.length; j++) {
        const a = es[i], b = es[j];
        const sa = LEDGER.sigmaLado(a, b.valor), sb = LEDGER.sigmaLado(b, a.valor);
        const n = Math.abs(a.valor - b.valor) / Math.sqrt(sa * sa + sb * sb);
        pares.push({ a: a.id, b: b.id, celulas: [a.publicado_por, b.publicado_por], sigma: n, independentes: !sobrepoe(a, b).length, mesmoModelo: a.modelo === b.modelo });
      }
      out[g] = { medidas: es.map((e) => ({ id: e.id, celula: e.publicado_por, valor: e.valor, sigma: e.sigma, sigma_mais: e.sigma_mais, sigma_menos: e.sigma_menos, modelo: e.modelo, tag: e.tag, rotulo: e.rotulo, dados_usados: e.dados_usados })), pares };
    }
    return out;
  }
  return { tensao, combinar, rdCMB, rdTardio, h0ReguaInversa, identidades, tabelaRede, sobrepoe, SIGMA_MNU_PREMISSA };
})();

/* ── 13 · ferramentas das células (tool use) ───────────────────────────────
 * Cada ferramenta é determinística e grava o que produz no ledger. Nenhuma
 * aceita número digitado pelo modelo como resultado: publicar_medida recebe
 * só o id de um registro e copia o valor do registro.
 */
const FERR = (() => {
  const LIMITE = 12000;
  const MAX_LINHAS = 50000; // linhas materializadas por arquivo de índice
  const tabelas = new Map(); // id do registro [D] → tabela {colunas, linhas}
  const compacto = (e) => {
    const o = { id: e.id, tag: e.tag, grandeza: e.grandeza, rotulo: e.rotulo || (e.texto ? e.texto.slice(0, 160) : undefined), valor: e.valor, sigma: e.sigma, sigma_mais: e.sigma_mais, sigma_menos: e.sigma_menos, unidade: e.unidade, modelo: e.modelo, celula: e.celula, publicado: e.publicado || undefined, avisos: e.avisos && e.avisos.length ? e.avisos : undefined, dados_usados: e.dados_usados, url: e.fonte && e.fonte.url };
    for (const k of Object.keys(o)) if (o[k] === undefined) delete o[k];
    return o;
  };
  /* Um registro [D] por arquivo (url + sha256) e célula; reaproveita se já existe. */
  function registrarArquivo(celula, url, sha256, rotulo, numeros, n) {
    const existente = LEDGER.listar({ celula, tag: 'D' }).find((e) => e.fonte && e.fonte.url === url && e.fonte.sha256 === sha256);
    if (existente) return existente;
    return LEDGER.registrar({ tag: 'D', celula, rotulo, n, fonte: { tipo: 'arquivo', url, sha256 }, numeros });
  }
  function escopoDe(celulaId, extras = []) {
    const ents = LEDGER.listar({}).filter((e) => e.celula === celulaId || e.celula === 'rede' || e.publicado || (e.fonte && e.fonte.tipo === 'literatura'));
    return { entradas: ents, extras };
  }
  const DEF = {
    inventario: {
      description: 'Lista o que esta célula tem: produtos de dados registrados, serviços TAP permitidos, pipelines validados, fatos e literatura atribuídos a ela.',
      input_schema: { type: 'object', properties: {}, additionalProperties: false },
      run(ctx) {
        const c = ctx.celula;
        return {
          celula: c.id, telescopio: c.nome, status: c.status,
          produtos: c.produtos.map((id) => ({ id, descricao: CELULAS.PRODUTOS[id].descricao, formato: CELULAS.PRODUTOS[id].formato, carregado: DADOS.memoria.has(CELULAS.PRODUTOS[id].url) })),
          tap: c.tap.map((s) => ({ servico: s, nome: TAP.SERVICOS[s].nome, conferido: TAP.SERVICOS[s].conferido })),
          pipelines: c.pipelines.map((p) => ({ nome: p, modelos: Object.keys(PIPE.MODELOS) })),
          fatos: LEDGER.listar({ celula: c.id, tag: 'F' }).map((e) => e.id),
          literatura: LEDGER.listar({}).filter((e) => e.fonte && e.fonte.tipo === 'literatura' && (e.celulas || []).includes(c.id)).map(compacto),
        };
      },
    },
    carregar_produto: {
      description: 'Baixa e lê um produto de dados registrado desta célula (ou um arquivo listado no índice de um produto do tipo índice). Grava um registro [D] com SHA-256.',
      input_schema: { type: 'object', properties: { produto: { type: 'string', description: 'id do produto (ver inventario)' }, arquivo: { type: 'string', description: 'URL de um arquivo listado no índice (só para produtos do tipo índice)' } }, required: ['produto'], additionalProperties: false },
      async run(ctx, inp) {
        const c = ctx.celula;
        if (!c.produtos.includes(inp.produto)) throw new Error('produto não pertence a esta célula: ' + inp.produto);
        const P = CELULAS.PRODUTOS[inp.produto];
        if (P.formato === 'indice') {
          if (!inp.arquivo) {
            const r = await DADOS.baixar(P.url, { maxBytes: 5e6 });
            const links = DADOS.indice(DADOS.texto(r.buf), P.url);
            return { indice: P.url, arquivos: links.slice(0, 200), nota: 'chame de novo com "arquivo" para ler um FITS/CSV listado' };
          }
          const base = CELULAS.DOMINIOS_INDICE[inp.produto];
          if (!String(inp.arquivo).startsWith(base)) throw new Error('arquivo fora do domínio permitido (' + base + ')');
          // arquivos de índice: até 60 MB, só as primeiras MAX_LINHAS linhas viram tabela, e o binário não fica retido em memória
          const r = await DADOS.baixar(inp.arquivo, { maxBytes: 60e6, semMemoria: true });
          const tab = /\.fits(\.gz)?$/i.test(inp.arquivo) ? DADOS.fits(r.buf, MAX_LINHAS) : DADOS.csv(DADOS.texto(r.buf), ',', MAX_LINHAS);
          const reg = registrarArquivo(c.id, inp.arquivo, r.sha256, 'arquivo ' + inp.arquivo.split('/').pop() + (tab.truncado ? ' (primeiras ' + MAX_LINHAS + ' linhas)' : ''), [tab.linhas.length, tab.colunas.length, tab.linhasNoArquivo].filter(Number.isFinite), tab.linhas.length);
          tabelas.set(reg.id, { colunas: tab.colunas, linhas: tab.linhas });
          return { id: reg.id, sha256: r.sha256, bytes: r.bytes, linhas: tab.linhas.length, linhas_no_arquivo: tab.linhasNoArquivo, truncado: tab.truncado || false, colunas: tab.colunas.slice(0, 80), amostra: tab.linhas.slice(0, 3) };
        }
        const r = await DADOS.baixar(P.url);
        let resumo = { bytes: r.bytes }, tab = null;
        if (P.formato === 'snana') { tab = DADOS.snana(DADOS.texto(r.buf)); resumo = { ...resumo, linhas: tab.linhas.length, colunas: tab.colunas }; }
        else if (P.formato === 'espacos') { tab = DADOS.espacos(DADOS.texto(r.buf)); resumo = { ...resumo, linhas: tab.linhas.length, colunas: tab.colunas.slice(0, 20) }; }
        else if (P.formato === 'texto') { const ls = DADOS.texto(r.buf).split(/\r?\n/).filter((l) => l.trim() && !l.trim().startsWith('#')); resumo = { ...resumo, linhas: ls.length, primeiras: ls.slice(0, 13) }; }
        else if (P.formato === 'npz') { const z = DADOS.zip(r.buf); resumo = { ...resumo, conteudo: Object.keys(z) }; }
        const reg = registrarArquivo(c.id, P.url, r.sha256, P.descricao, [r.bytes, resumo.linhas].filter((x) => typeof x === 'number'), resumo.linhas);
        if (resumo.primeiras) reg.numeros_guarda.push(...resumo.primeiras.flatMap((l) => l.split(/\s+/).map(Number).filter(Number.isFinite)));
        if (tab) tabelas.set(reg.id, tab);
        return { id: reg.id, sha256: r.sha256, ...resumo };
      },
    },
    consultar_tap: {
      description: 'Executa uma consulta ADQL (somente SELECT) num serviço TAP permitido a esta célula. Grava a resposta como [D] com SHA-256. Use TOP n ou max_linhas para limitar.',
      input_schema: { type: 'object', properties: { servico: { type: 'string' }, adql: { type: 'string' }, max_linhas: { type: 'integer', minimum: 1, maximum: 50000 } }, required: ['servico', 'adql'], additionalProperties: false },
      async run(ctx, inp) {
        if (!ctx.celula.tap.includes(inp.servico)) throw new Error('serviço TAP não permitido a esta célula: ' + inp.servico + ' (permitidos: ' + ctx.celula.tap.join(', ') + ')');
        const r = await TAP.consultar(inp.servico, inp.adql, inp.max_linhas || 2000);
        // o ADQL fica em fonte.adql e não entra na guarda: números escolhidos pelo modelo na consulta não viram registro
        const reg = LEDGER.registrar({ tag: 'D', celula: ctx.celula.id, rotulo: 'resposta TAP de ' + inp.servico + ' (consulta em fonte.adql)', n: r.linhas.length, fonte: { tipo: 'tap', url: r.url, sha256: r.sha256, adql: inp.adql }, numeros: [r.linhas.length] });
        tabelas.set(reg.id, { colunas: r.colunas, linhas: r.linhas });
        return { id: reg.id, linhas: r.linhas.length, colunas: r.colunas, amostra: r.linhas.slice(0, 5), ms: r.ms };
      },
    },
    estatistica: {
      description: 'Estatística descritiva (n, média, desvio, mínimo, quartis, máximo) de colunas numéricas de uma tabela [D] já carregada. Grava [C].',
      input_schema: { type: 'object', properties: { id_tabela: { type: 'string' }, colunas: { type: 'array', items: { type: 'string' } } }, required: ['id_tabela'], additionalProperties: false },
      run(ctx, inp) {
        const tab = tabelas.get(inp.id_tabela);
        if (!tab) throw new Error('tabela não carregada nesta sessão: ' + inp.id_tabela);
        const est = PIPE.estatistica(tab, inp.colunas);
        const nums = Object.values(est).flatMap((s) => Object.values(s));
        const reg = LEDGER.registrar({ tag: 'C', celula: ctx.celula.id, grandeza: null, rotulo: 'estatística de ' + inp.id_tabela, entradas: [inp.id_tabela], numeros: nums, metodo: 'estatística descritiva', fonte: { tipo: 'calculo', descricao: 'estatística descritiva' }, extra: est });
        return { id: reg.id, estatistica: est };
      },
    },
    executar_pipeline: {
      description: 'Roda um pipeline validado desta célula sobre os dados oficiais. Modelos: lcdm (mínimo + Laplace), wcdm e w0wacdm (MCMC, mais lento). Grava cada parâmetro como [C].',
      input_schema: { type: 'object', properties: { pipeline: { type: 'string' }, modelo: { type: 'string', enum: ['lcdm', 'wcdm', 'w0wacdm'] }, covariancia_sn: { type: 'string', enum: ['STAT+SYS', 'STATONLY'] }, radiacao: { type: 'boolean' } }, required: ['pipeline', 'modelo'], additionalProperties: false },
      async run(ctx, inp) {
        if (!ctx.celula.pipelines.includes(inp.pipeline)) throw new Error('pipeline não validado para esta célula: ' + inp.pipeline);
        const comp = { bao_desi: ['bao'], sn_dovekie: ['sn'], shoes_h0: ['shoes'] }[inp.pipeline];
        const res = await PIPE.ajustar(comp, inp.modelo, { covSN: inp.covariancia_sn, radiacao: inp.radiacao, sinal: ctx.sinal });
        const entradasD = res.arquivos.map((a) => registrarArquivo(ctx.celula.id, a.url, a.sha256, 'arquivo ' + decodeURIComponent(a.url.split('/').pop()), [a.bytes]).id);
        const regs = PIPE.registrar(ctx.celula.id, res, entradasD);
        return { pipeline: inp.pipeline, modelo: res.rotuloModelo, metodo: res.metodo, chi2: res.chi2min, n: res.n, ndof: res.ndof, p: res.p, aceitacao: res.aceitacao, ms: res.ms, registros: regs.map(compacto), configuracao: res.configuracao };
      },
    },
    recalcular: {
      description: 'Refaz um cálculo [C] com ajustes numéricos independentes (passo de integração, ponto de partida) e compara. Grava [C] da verificação.',
      input_schema: { type: 'object', properties: { id: { type: 'string' } }, required: ['id'], additionalProperties: false },
      async run(ctx, inp) {
        const e = LEDGER.obter(inp.id);
        if (!e || !e.refazer) throw new Error('registro sem receita de recálculo: ' + inp.id);
        const res = await PIPE.ajustar(e.refazer.componentes, e.refazer.modelo, { ...e.refazer.opcoes, variante: true, semente: 7, sinal: ctx.sinal });
        const v = res.parametros[e.grandeza];
        const s = LEDGER.sigmaSimetrico(e);
        const dif = Math.abs(v.valor - e.valor) / s;
        // ajuste determinístico (Laplace): o recálculo tem de coincidir a 0,1σ. MCMC: medianas de cadeias com
        // sementes diferentes diferiram até 0,13σ (BAO+SN w0waCDM, 3 cadeias de 8000 passos, medido em 16/out/2026),
        // então o limite é 0,3σ
        const limite = res.aceitacao ? 0.3 : 0.1;
        const reg = LEDGER.registrar({ tag: 'C', celula: ctx.celula.id, grandeza: null, rotulo: 'recálculo de ' + e.id, valor: v.valor, entradas: [e.id], numeros: [dif, limite, v.valor, v.sigma, v.sigma_mais, v.sigma_menos].filter(Number.isFinite), ok: dif < limite, metodo: 'refeito com dz menor e outro ponto de partida' + (res.aceitacao ? ' e outra semente de MCMC' : '') + '; aprovado se a diferença for menor que ' + U.fmt(limite, 1) + 'σ', fonte: { tipo: 'calculo', descricao: 'Polo V · recálculo' } });
        return { id: reg.id, original: { valor: e.valor, sigma: s }, refeito: v, diferenca_em_sigma: dif, limite_em_sigma: limite, aprovado: dif < limite };
      },
    },
    polo_v: {
      description: 'Verificações determinísticas do Polo V: identidades (Ω_m=(ω_b+ω_c+ω_ν)/h², S₈=σ₈√(Ω_m/0,3), idade), rd_cmb (Aubourg 2015 com ω_b, ω_c), rd_tardio (h·r_d ÷ h), h0_regua_inversa (h·r_d ÷ r_d). Recebe só ids do registro.',
      input_schema: { type: 'object', properties: { checagem: { type: 'string', enum: ['identidades', 'rd_cmb', 'rd_tardio', 'h0_regua_inversa'] }, ids: { type: 'array', items: { type: 'string' } } }, required: ['checagem'], additionalProperties: false },
      run(ctx, inp) {
        const ids = inp.ids || [];
        let regs;
        if (inp.checagem === 'identidades') regs = POLOV.identidades(ctx.celula.id);
        else if (inp.checagem === 'rd_cmb') regs = [POLOV.rdCMB(ids[0], ids[1], ctx.celula.id)];
        else if (inp.checagem === 'rd_tardio') regs = [POLOV.rdTardio(ids[0], ids[1], ctx.celula.id)];
        else regs = [POLOV.h0ReguaInversa(ids[0], ids[1], ctx.celula.id)];
        return { registros: regs.map((e) => ({ ...compacto(e), metodo: e.metodo, premissas: e.premissas, ok: e.ok })) };
      },
    },
    tensao: {
      description: 'Tensão em σ entre dois registros da mesma grandeza (assimetria tratada pelo lado voltado ao outro valor). Avisa se não forem independentes ou se os modelos diferirem. Grava [C].',
      input_schema: { type: 'object', properties: { id_a: { type: 'string' }, id_b: { type: 'string' } }, required: ['id_a', 'id_b'], additionalProperties: false },
      run(ctx, inp) { const e = POLOV.tensao(inp.id_a, inp.id_b, ctx.celula.id); return { ...compacto(e), entradas: e.entradas, avisos: e.avisos, p: e.p }; },
    },
    ledger: {
      description: 'Lista registros: da célula, da rede (publicados), literatura ou tudo. Use os ids como etiqueta ao citar números.',
      input_schema: { type: 'object', properties: { escopo: { type: 'string', enum: ['celula', 'rede', 'literatura', 'tudo'] }, grandeza: { type: 'string' } }, additionalProperties: false },
      run(ctx, inp) {
        const esc = inp.escopo || 'celula';
        let es = LEDGER.listar({ grandeza: inp.grandeza });
        if (esc === 'celula') es = es.filter((e) => e.celula === ctx.celula.id);
        else if (esc === 'rede') es = es.filter((e) => e.publicado || e.celula === 'rede');
        else if (esc === 'literatura') es = es.filter((e) => e.fonte && e.fonte.tipo === 'literatura');
        return { registros: es.slice(-120).map(compacto) };
      },
    },
    fatos: {
      description: 'Fatos verificados sobre este telescópio, com fonte.',
      input_schema: { type: 'object', properties: {}, additionalProperties: false },
      run(ctx) { return { fatos: LEDGER.listar({ celula: ctx.celula.id, tag: 'F' }).map((e) => ({ id: e.id, texto: e.texto, url: e.fonte.url })) }; },
    },
    literatura: {
      description: 'Valores publicados e conferidos (literatura), com fonte, modelo e conjuntos de dados usados.',
      input_schema: { type: 'object', properties: { grandeza: { type: 'string' } }, additionalProperties: false },
      run(ctx, inp) { return { registros: LEDGER.listar({ grandeza: inp.grandeza }).filter((e) => e.fonte && e.fonte.tipo === 'literatura' && !e.copia_de).map(compacto) }; },
    },
    rede: {
      description: 'Medidas publicadas por todas as células, agrupadas por grandeza, com tensões par a par e marcação de independência.',
      input_schema: { type: 'object', properties: { grandeza: { type: 'string' } }, additionalProperties: false },
      run(ctx, inp) { const t = POLOV.tabelaRede(); return inp.grandeza ? { [inp.grandeza]: t[inp.grandeza] || null } : t; },
    },
    publicar_medida: {
      description: 'Publica na rede um registro [C] ou [D] desta célula (só grandezas cosmológicas com incerteza). O valor vem do registro, nunca do texto.',
      input_schema: { type: 'object', properties: { id: { type: 'string' } }, required: ['id'], additionalProperties: false },
      run(ctx, inp) {
        const e = LEDGER.publicar(inp.id, ctx.celula.id);
        const quadro = ctx.barramento.enviar(URB1.TIPO.MEDIDA, ctx.celula.id, '*', { id: e.id, grandeza: e.grandeza, valor: e.valor, sigma: e.sigma, sigma_mais: e.sigma_mais, sigma_menos: e.sigma_menos, modelo: e.modelo, tag: e.tag });
        return { publicado: compacto(e), urb1: quadro };
      },
    },
    guarda: {
      description: 'Confere um texto contra o registro: cada número precisa existir num registro visível para a célula. Devolve as violações.',
      input_schema: { type: 'object', properties: { texto: { type: 'string' } }, required: ['texto'], additionalProperties: false },
      run(ctx, inp) { return LEDGER.guarda(inp.texto, escopoDe(ctx.celula.id, [...(ctx.extrasGuarda || []), ...(ctx.numerosCelula || [])])); },
    },
  };
  function esquemas(nomes) { return nomes.map((n) => ({ name: n, description: DEF[n].description, input_schema: DEF[n].input_schema })); }
  async function executar(nome, entrada, ctx) {
    const d = DEF[nome];
    if (!d) return { erro: 'ferramenta desconhecida: ' + nome };
    if (!ctx.permitidas.includes(nome)) return { erro: 'ferramenta não permitida a este papel: ' + nome };
    if (ctx.sinal && ctx.sinal.aborted) return { erro: 'ciclo interrompido' };
    try {
      const r = await d.run(ctx, entrada || {});
      return { resultado: r };
    } catch (e) {
      return { erro: String(e && e.message || e) };
    }
  }
  return { DEF, esquemas, executar, escopoDe, tabelas, compacto, LIMITE };
})();

/* ── 14 · Claude via Messages API (e simulador determinístico) ─────────────
 * POST https://api.anthropic.com/v1/messages, anthropic-version 2023-06-01.
 * Sonnet 5.5 pensa por padrão (adaptive) e recusa tool_choice forçado: usa-se
 * tool_choice auto. Blocos de thinking voltam inalterados no turno seguinte
 * (o assistant recebe response.content inteiro).
 */
const IA = (() => {
  const limitar = U.criarLimitador(CFG.CONCORRENCIA, CFG.RPM);
  const custo = { entrada: 0, saida: 0, cache_escrita: 0, cache_leitura: 0, usd: 0, chamadas: 0, erros: 0, reenvios: 0 };
  class ErroTeto extends Error {}
  class ErroAPI extends Error { constructor(status, msg, tipo) { super(msg); this.status = status; this.tipo = tipo; } }
  const MSG = {
    400: 'requisição inválida', 401: 'chave da API recusada (confira ANTHROPIC_API_KEY)', 403: 'sem permissão para este modelo ou recurso',
    404: 'modelo ou endpoint não encontrado (confira MODELO)', 413: 'requisição grande demais', 429: 'limite de taxa da API',
    500: 'erro interno da API', 529: 'API sobrecarregada',
  };
  function traduzir(status, corpo) {
    let tipo = '', msg = '';
    try { const j = JSON.parse(corpo); tipo = (j.error && j.error.type) || ''; msg = (j.error && j.error.message) || ''; } catch (e) { msg = String(corpo).slice(0, 300); }
    return { tipo, msg: (MSG[status] || 'HTTP ' + status) + (msg ? ' — ' + msg : '') };
  }
  function cobrar(uso) {
    if (!uso) return;
    custo.entrada += uso.input_tokens || 0;
    custo.cache_escrita += uso.cache_creation_input_tokens || 0;
    custo.cache_leitura += uso.cache_read_input_tokens || 0;
    custo.saida += uso.output_tokens || 0;
    custo.usd = ((custo.entrada + custo.cache_escrita * CFG.FATOR_CACHE_ESCRITA + custo.cache_leitura * CFG.FATOR_CACHE_LEITURA) / 1e6) * CFG.PRECO_ENTRADA + (custo.saida / 1e6) * CFG.PRECO_SAIDA;
  }
  const espera = (k) => Math.min(60000, 2000 * Math.pow(2, k)) + Math.floor(Math.random() * 1000);
  function zerarCusto() { Object.assign(custo, { entrada: 0, saida: 0, cache_escrita: 0, cache_leitura: 0, usd: 0, chamadas: 0, erros: 0, reenvios: 0 }); }

  async function mensagem(corpo, sinal) {
    if (custo.usd >= CFG.TETO_USD) throw new ErroTeto('teto de gasto atingido: US$ ' + custo.usd.toFixed(2) + ' (TETO_USD=' + CFG.TETO_USD + ')');
    if (sinal && sinal.aborted) throw new Error('ciclo interrompido');
    // simulado: conta chamadas, não cobra (os tokens do simulador não são medida de nada)
    if (CFG.MODO === 'simulado') { custo.chamadas++; const r = SIM.responder(corpo); await U.dormir(5); return r; }
    for (let tentativa = 0; ; tentativa++) {
      let r, texto;
      try {
        r = await limitar(() => fetch(CFG.ANTHROPIC_URL, {
          method: 'POST',
          headers: { 'x-api-key': CFG.ANTHROPIC_API_KEY, 'anthropic-version': CFG.ANTHROPIC_VERSION, 'content-type': 'application/json' },
          body: JSON.stringify(corpo),
          signal: sinal ? AbortSignal.any([sinal, AbortSignal.timeout(CFG.TIMEOUT_API_MS)]) : AbortSignal.timeout(CFG.TIMEOUT_API_MS),
        }));
        texto = await r.text();
      } catch (e) {
        if (sinal && sinal.aborted) throw new Error('ciclo interrompido');
        if (tentativa < 4) { custo.reenvios++; await U.dormir(espera(tentativa)); continue; }
        throw new ErroAPI(0, 'falha de rede ao chamar a API: ' + e.message);
      }
      custo.chamadas++;
      if (r.ok) {
        const j = JSON.parse(texto);
        cobrar(j.usage);
        return j;
      }
      custo.erros++;
      const t = traduzir(r.status, texto);
      if ((r.status === 429 || r.status === 529 || r.status >= 500) && tentativa < 6) {
        custo.reenvios++;
        const ra = Number(r.headers.get('retry-after'));
        await U.dormir(Number.isFinite(ra) && ra > 0 ? Math.min(ra * 1000, 120000) : espera(tentativa));
        continue;
      }
      throw new ErroAPI(r.status, t.msg, t.tipo);
    }
  }

  /* Cache de prompt: dois pontos de corte (dos 4 permitidos) — o system e o último
     bloco da última mensagem do usuário; o servidor da API procura acertos nas
     posições anteriores sozinho. Nunca marca bloco de thinking (fica no turno do
     assistant, que não recebe marca). Prefixos curtos (< 512 tokens) só não são
     cacheados, sem erro. */
  const MARCA = { type: 'ephemeral' };
  function marcarCache(msgs) {
    for (const m of msgs) if (m.role === 'user' && Array.isArray(m.content)) for (const b of m.content) delete b.cache_control;
    const u = msgs[msgs.length - 1];
    if (!u || u.role !== 'user') return;
    if (typeof u.content === 'string') u.content = [{ type: 'text', text: u.content }];
    u.content[u.content.length - 1].cache_control = { ...MARCA };
  }
  /* Laço de agente com ferramentas. */
  async function agente({ sistema, usuario, ferramentas, ctx, esforco, maxPassos, aoPasso }) {
    const tools = FERR.esquemas(ferramentas);
    const msgs = [{ role: 'user', content: usuario }];
    const chamadas = [], numeros = [];
    let passos = 0, ultimo = null;
    const system = CFG.CACHE_PROMPT ? [{ type: 'text', text: sistema, cache_control: { ...MARCA } }] : sistema;
    const base = { model: CFG.MODELO, max_tokens: CFG.MAX_TOKENS, system, tools, tool_choice: { type: 'auto' }, output_config: { effort: esforco || CFG.ESFORCO } };
    for (;;) {
      if (CFG.CACHE_PROMPT) marcarCache(msgs);
      const resp = await mensagem({ ...base, messages: msgs }, ctx.sinal);
      ultimo = resp;
      msgs.push({ role: 'assistant', content: resp.content });
      const usos = (resp.content || []).filter((b) => b.type === 'tool_use');
      if (resp.stop_reason !== 'tool_use' || !usos.length) break;
      const resultados = [];
      for (const u of usos) {
        const out = await FERR.executar(u.name, u.input, ctx);
        chamadas.push({ nome: u.name, entrada: u.input, erro: out.erro || null });
        if (!out.erro && numeros.length < 30000) for (const v of LEDGER.numerosDeJSON(out.resultado, 5000)) numeros.push(v);
        if (aoPasso) aoPasso({ nome: u.name, entrada: u.input, erro: out.erro || null, resultado: out.resultado });
        resultados.push({ type: 'tool_result', tool_use_id: u.id, content: U.json(out.erro ? { erro: out.erro } : out.resultado, FERR.LIMITE), is_error: !!out.erro });
      }
      passos++;
      if (passos >= maxPassos) {
        msgs.push({ role: 'user', content: [...resultados, { type: 'text', text: 'Limite de ' + maxPassos + ' rodadas de ferramentas atingido. Responda agora, sem ferramentas, no formato pedido.' }] });
        if (CFG.CACHE_PROMPT) marcarCache(msgs);
        const rf = await mensagem({ ...base, messages: msgs, tool_choice: { type: 'none' } }, ctx.sinal);
        msgs.push({ role: 'assistant', content: rf.content });
        ultimo = rf;
        break;
      }
      msgs.push({ role: 'user', content: resultados });
    }
    const texto = (ultimo.content || []).filter((b) => b.type === 'text').map((b) => b.text).join('\n').trim();
    return { texto, stop: ultimo.stop_reason, passos, chamadas, turnos: msgs.length, numeros };
  }
  function digestDe(texto) {
    const m = /DIGEST\s*:\s*([^\n]+)/i.exec(texto || '');
    return m ? m[1].split('|').map((s) => s.trim().slice(0, 300)).filter(Boolean).slice(0, 8) : [];
  }
  return { mensagem, agente, digestDe, custo, zerarCusto, ErroTeto, ErroAPI, limitar, traduzir };
})();

/* ── simulador: mesmo formato de resposta da Messages API, sem rede ────────
 * Segue um roteiro por papel, chama as ferramentas reais e escreve números
 * apenas a partir dos resultados das ferramentas (com o id do registro).
 * SIMULAR_ALUCINACAO=<célula> faz o TEÓRICO dessa célula inventar um número,
 * para testar a guarda e a supressão no gossip.
 */
const SIM = (() => {
  let seq = 0;
  const id = (p) => p + '_' + (++seq).toString(36).padStart(6, '0');
  const textoDe = (sistema) => (Array.isArray(sistema) ? sistema.map((b) => b.text || '').join('\n') : String(sistema || ''));
  function papelDe(sistema) { const m = /PAPEL_ID=([a-z]+)/.exec(textoDe(sistema)); return m ? m[1] : 'relator'; }
  function celulaDe(sistema) { const m = /CELULA_ID=([a-z]+)/.exec(textoDe(sistema)); return m ? m[1] : ''; }
  function resultados(msgs) {
    const out = [];
    for (const m of msgs) if (m.role === 'user' && Array.isArray(m.content)) for (const b of m.content) if (b.type === 'tool_result') { try { out.push(JSON.parse(b.content)); } catch (e) { out.push({ bruto: b.content }); } }
    return out;
  }
  const temFerr = (corpo, n) => (corpo.tools || []).some((t) => t.name === n);
  const fmtReg = (r) => {
    if (r.valor === undefined) return '[' + r.id + ']';
    const s = r.sigma > 0 ? r.sigma : r.sigma_mais > 0 ? (r.sigma_mais + r.sigma_menos) / 2 : NaN;
    const c = U.casasPara(s);
    const val = U.fmt(r.valor, c);
    if (r.sigma > 0) return val + ' ± ' + U.fmt(r.sigma, c) + ' [' + r.id + ']';
    if (r.sigma_mais > 0) return val + ' (+' + U.fmt(r.sigma_mais, c) + '/−' + U.fmt(r.sigma_menos, c) + ') [' + r.id + ']';
    return val + ' [' + r.id + ']';
  };
  function plano(papel, cel, corpo, rs) {
    const n = rs.length;
    const c = CELULAS.porId[cel] || { produtos: [], pipelines: [], tap: [] };
    const passos = [];
    if (papel === 'coletor') { passos.push(['inventario', {}]); for (const p of c.produtos.filter((p) => !/cov/.test(p) && CELULAS.PRODUTOS[p].formato !== 'indice')) passos.push(['carregar_produto', { produto: p }]); passos.push(['fatos', {}]); }
    if (papel === 'analista') { for (const p of c.pipelines) passos.push(['executar_pipeline', { pipeline: p, modelo: 'lcdm' }]); if (!c.pipelines.length) passos.push(['ledger', { escopo: 'celula' }]); }
    if (papel === 'teorico') { passos.push(['ledger', { escopo: 'celula' }]); passos.push(['literatura', {}]); }
    if (papel === 'cetico') {
      passos.push(['ledger', { escopo: 'celula' }]);
      if (c.pipelines.includes('sn_dovekie')) passos.push(['executar_pipeline', { pipeline: 'sn_dovekie', modelo: 'lcdm', covariancia_sn: 'STATONLY' }]);
    }
    if (papel === 'verificador') passos.push(['ledger', { escopo: 'celula' }]);
    if (papel === 'auditor') passos.push(['ledger', { escopo: 'celula' }]);
    if (papel === 'embaixador') passos.push(['rede', {}]);
    if (papel === 'relator') passos.push(['ledger', { escopo: 'celula' }]);
    if (papel === 'redutor') passos.push(['rede', {}]);
    // passos dependentes de resultados anteriores
    const regsCel = rs.flatMap((r) => (r.registros || [])).filter((e) => e.celula === cel);
    const calc = regsCel.filter((e) => e.tag === 'C' && e.grandeza && e.valor !== undefined);
    if (papel === 'verificador' && n >= 1) for (const e of calc.slice(0, 2)) passos.push(['recalcular', { id: e.id }]);
    if (papel === 'cetico' && n >= 1) {
      const lit = LEDGER.listar({}).filter((e) => e.fonte && e.fonte.tipo === 'literatura');
      const e = calc.find((x) => lit.some((l) => l.grandeza === x.grandeza));
      if (e) passos.push(['tensao', { id_a: e.id, id_b: lit.find((l) => l.grandeza === e.grandeza).id }]);
    }
    if (papel === 'auditor' && n >= 1) {
      const c0 = corpo.messages[0].content;
      const m = /<<TEXTO_A_AUDITAR>>([\s\S]*?)<<FIM>>/.exec(typeof c0 === 'string' ? c0 : (Array.isArray(c0) ? c0.filter((b) => b.type === 'text').map((b) => b.text).join('\n') : ''));
      if (m) passos.push(['guarda', { texto: m[1].slice(0, 20000) }]);
    }
    if (papel === 'embaixador' && n >= 1) {
      const t = rs[0] || {};
      const minhas = new Set(LEDGER.listar({ publicado: true }).filter((e) => e.publicado_por === cel).map((e) => e.id));
      for (const g of Object.keys(t)) {
        const ms = (t[g] && t[g].medidas) || [];
        const a = ms.find((x) => minhas.has(x.id)), b = ms.find((x) => !minhas.has(x.id));
        if (a && b) passos.push(['tensao', { id_a: a.id, id_b: b.id }]);
      }
    }
    if (papel === 'relator' && n >= 1) {
      const pub = calc.filter((e) => LEDGER.GRANDEZAS_PUBLICAVEIS[e.grandeza] && !e.publicado && (e.sigma > 0 || e.sigma_mais > 0) && !/\[checagem:/.test(e.rotulo || ''));
      for (const e of pub) passos.push(['publicar_medida', { id: e.id }]);
      if (!calc.length) {
        const lit = LEDGER.listar({}).filter((e) => e.fonte && e.fonte.tipo === 'literatura' && (e.celulas || []).includes(cel) && e.celula === cel && (!e.publicado || e.publicado_por === cel));
        for (const e of lit.slice(0, 3)) passos.push(['publicar_medida', { id: e.id }]);
      }
    }
    return passos.filter((p) => temFerr(corpo, p[0]));
  }
  function textoFinal(papel, cel, rs) {
    const regs = [];
    for (const r of rs) {
      if (r.registros) regs.push(...r.registros);
      if (r.publicado) regs.push(r.publicado);
      if (r.id && r.valor !== undefined) regs.push(r);
      if (r.literatura) regs.push(...r.literatura);
      if (!r.registros && !r.id) for (const v of Object.values(r)) if (v && Array.isArray(v.medidas)) regs.push(...v.medidas);
    }
    const vistos = new Set(), linhas = [];
    for (const e of regs) {
      if (!e.id || vistos.has(e.id) || e.valor === undefined) continue;
      vistos.add(e.id);
      linhas.push((e.rotulo || e.grandeza || e.id) + ': ' + fmtReg(e));
      if (linhas.length >= 6) break;
    }
    let corpo = 'Papel ' + papel.toUpperCase() + ' da célula ' + cel + ' (simulado).\n' + (linhas.length ? linhas.map((l) => '- ' + l).join('\n') : '- [L] nenhum número disponível nesta etapa');
    if (CFG.SIMULAR_ALUCINACAO === cel && papel === 'teorico') corpo += '\n- Ω_m = 0,412 ± 0,003 segundo a calibração interna da célula.';
    const dig = linhas.slice(0, 5).map((l) => l.replace(/\s+/g, ' ').slice(0, 140));
    if (!dig.length) dig.push('[L] sem medida própria nesta etapa', 'célula ' + cel + ' segue em ingestão e literatura');
    return corpo + '\nDIGEST: ' + dig.join(' | ');
  }
  function responder(corpo) {
    const papel = papelDe(corpo.system), cel = celulaDe(corpo.system);
    const rs = resultados(corpo.messages);
    const feitos = corpo.messages.filter((m) => m.role === 'assistant').length;
    const pl = plano(papel, cel, corpo, rs);
    const thinking = { type: 'thinking', thinking: '', signature: 'sim-' + U.sha256(papel + cel + feitos).slice(0, 24) };
    const uso = { input_tokens: Math.round(JSON.stringify(corpo.messages).length / 4) + 800, output_tokens: 300, cache_creation_input_tokens: 0, cache_read_input_tokens: 0 };
    const naoFeitos = pl.slice(feitos);
    if (naoFeitos.length && corpo.tool_choice && corpo.tool_choice.type !== 'none' && feitos < 6) {
      const [nome, entrada] = naoFeitos[0];
      return { id: id('msg'), type: 'message', role: 'assistant', model: corpo.model, stop_reason: 'tool_use', content: [thinking, { type: 'tool_use', id: id('toolu'), name: nome, input: entrada }], usage: uso };
    }
    return { id: id('msg'), type: 'message', role: 'assistant', model: corpo.model, stop_reason: 'end_turn', content: [thinking, { type: 'text', text: textoFinal(papel, cel, rs) }], usage: uso };
  }
  return { responder };
})();

/* ── 15 · os 8 papéis de cada célula ───────────────────────────────────────
 * Pares adversariais: ANALISTA × CÉTICO, TEÓRICO × AUDITOR; VERIFICADOR é a
 * porta determinística; EMBAIXADOR fala com a rede; RELATOR decide o que sai.
 */
const PAPEIS = (() => {
  const L = [
    { id: 'coletor', nome: 'COLETOR', sigla: 'COL', ferramentas: ['inventario', 'carregar_produto', 'consultar_tap', 'fatos', 'literatura'],
      missao: 'Levante o que esta célula consegue medir agora. Rode inventario; carregue os produtos registrados (arquivos de covariância grandes são lidos pelos pipelines, não precisa abri-los); se houver serviço TAP permitido, faça no máximo duas consultas ADQL enxutas (TOP ≤ 2000) que sirvam a matéria escura ou energia escura; leia os fatos.',
      formato: 'Seções curtas: DADOS CARREGADOS (id de cada registro) · O QUE FALTA [L] · O QUE O ANALISTA DEVE RODAR.' },
    { id: 'analista', nome: 'ANALISTA', sigla: 'ANA', ferramentas: ['inventario', 'executar_pipeline', 'estatistica', 'ledger'],
      missao: 'Rode os pipelines validados desta célula: comece por lcdm; rode wcdm ou w0wacdm quando servir à pergunta de energia escura. Sem pipeline validado, diga isso com [L] e use só estatística descritiva das tabelas carregadas.',
      formato: 'RESULTADOS (cada parâmetro com valor, incerteza, id, χ² e n) · O QUE OS RESULTADOS NÃO DIZEM.' },
    { id: 'teorico', nome: 'TEÓRICO', sigla: 'TEO', ferramentas: ['ledger', 'literatura', 'fatos', 'polo_v'], esforcoCritico: false,
      missao: 'Interprete para matéria escura e energia escura os resultados [C] e a literatura [D] desta célula: o que cada número significa, em que modelo vale, que premissas carrega e onde a física pode estar além de ΛCDM. Rode polo_v com checagem "identidades" quando houver ω_b, ω_c, H₀ e Ω_m de uma mesma fonte.',
      formato: 'LEITURA FÍSICA · PREMISSAS · O QUE MUDARIA A CONCLUSÃO.' },
    { id: 'cetico', nome: 'CÉTICO', sigla: 'CET', ferramentas: ['ledger', 'literatura', 'executar_pipeline', 'recalcular', 'tensao'], esforcoCritico: true,
      missao: 'Ataque os resultados desta célula: sistemáticas, dependência de modelo, priors, calibração, independência dos dados. Para cada crítica, rode uma checagem determinística quando possível (covariância só estatística, recalcular, tensão contra a literatura) e diga se a crítica se sustenta pelos números.',
      formato: 'CRÍTICAS (uma por linha: crítica → checagem → resultado com id → se sustenta ou não).' },
    { id: 'verificador', nome: 'VERIFICADOR', sigla: 'VER', ferramentas: ['ledger', 'recalcular', 'polo_v', 'tensao'],
      missao: 'Recalcule com ajuste numérico independente cada registro [C] publicável desta célula (recalcular) e rode o Polo V aplicável. Aprove ou reprove cada registro, com a diferença em σ do recálculo.',
      formato: 'TABELA EM TEXTO: id · grandeza · diferença do recálculo em σ (com o id da verificação) · aprovado/reprovado.' },
    { id: 'auditor', nome: 'AUDITOR', sigla: 'AUD', ferramentas: ['ledger', 'guarda'],
      missao: 'Audite os textos dos outros papéis desta célula (entre <<TEXTO_A_AUDITAR>> e <<FIM>>). Rode guarda nesse texto; liste números sem registro, ids inexistentes, atribuições sem fonte e lacunas [L] que viraram certeza. A guarda determinística já rodou por papel e o resultado está abaixo; confirme ou aponte o que ela não pega.',
      formato: 'VIOLAÇÕES · RESSALVAS · VEREDITO: aprovado | aprovado com ressalvas | reprovado.' },
    { id: 'embaixador', nome: 'EMBAIXADOR', sigla: 'EMB', ferramentas: ['ledger', 'rede', 'tensao', 'literatura'],
      missao: 'Leia a rede (medidas publicadas por todas as células) e os digests das células vizinhas abaixo. Para cada grandeza em comum, calcule a tensão entre a medida desta célula e as das outras (tensao), respeitando os avisos de independência e de modelo. Mantenha o que resiste, corte o que a rede refuta com números, e não convirja por educação.',
      formato: 'CONFIRMA · CONTRADIZ · NÃO DÁ PARA AVALIAR (cada item com ids).' },
    { id: 'relator', nome: 'RELATOR', sigla: 'REL', ferramentas: ['ledger', 'rede', 'publicar_medida', 'fatos'], esforcoCritico: true,
      missao: 'Escreva o veredito da célula: o que foi medido [C], o que foi reportado [D], as lacunas [L], as tensões com a rede e a confiança. Publique na rede (publicar_medida) apenas registros com incerteza, aprovados pelo VERIFICADOR e sem violação apontada pelo AUDITOR; não publique variantes de checagem (rótulo com "[checagem: …]"), que servem só para testar robustez; na falta de cálculo próprio, publique a literatura atribuída a esta célula.',
      formato: 'MEDIDO · REPORTADO · LACUNAS · TENSÕES · CONFIANÇA · PUBLICADO (ids).' },
  ];
  const porId = Object.fromEntries(L.map((p) => [p.id, p]));
  function sistema(papel, cel, ciclo) {
    return [
      'Você é o ' + papel.nome + ' da célula C' + String(cel.n).padStart(2, '0') + ' · ' + cel.nome + ' (' + cel.telescopio + ', ' + cel.banda + ') no PARALLAX · Células: 17 células, uma por telescópio, cada uma com 8 instâncias Claude Sonnet, analisando matéria escura (MD) e energia escura (EE).',
      'PAPEL_ID=' + papel.id + ' CELULA_ID=' + cel.id,
      'Sondas desta célula: ' + cel.sondas + '. Situação do pipeline: ' + cel.status + '.',
      '',
      'REGRA DE OURO (Polo V): nenhum número nasce de você. Todo número que você escrever precisa ter vindo de uma ferramenta nesta conversa ou do registro, seguido do id do registro entre colchetes, no formato: Ω_m = valor ± incerteza [C-1a2b3c4d]. Prefixos: C- calculado aqui; D- dado, arquivo ou literatura; F- fato verificado. Quando faltar dado, escreva [L] e diga o que falta. Não cite artigo, catálogo, versão ou valor que não esteja no registro. Interpretação física sem número é permitida e deve vir marcada como interpretação.',
      'Uma guarda determinística confere cada número que você escrever; número sem registro conta como violação e reduz o peso desta célula na rede, até suprimi-la do gossip. Valor e incerteza escritos juntos (± ou +σ₊/−σ₋) precisam vir do mesmo registro. Não faça conta de cabeça: diferença, razão, média, combinação ou tensão saem das ferramentas (tensao, polo_v, recalcular). Ficam isentos só inteiros até 20, anos e níveis de confiança (68%, 95%).',
      '',
      'SEU PAPEL: ' + papel.missao,
      'FORMATO: ' + papel.formato + ' Escreva em português, frases curtas. Termine com uma única linha:',
      'DIGEST: asserção 1 | asserção 2 | ... (5 a 8 asserções curtas; cada número com seu id)',
      '',
      'Pergunta do ciclo: ' + ciclo.objetivo,
    ].join('\n');
  }
  return { L, porId, sistema };
})();

/* ── 16 · orquestração: fases, gossip, peso de influência, redução ─────────
 *   fase 01 · local     COLETOR → ANALISTA → (TEÓRICO ∥ CÉTICO) → VERIFICADOR
 *                       → AUDITOR → RELATOR, em todas as células em paralelo
 *   fase 02 · gossip    EMBAIXADOR troca digests por quadros URB1 com vizinhas;
 *                       célula com peso < LIMIAR_PESO não fala na rede
 *   fase 03 · veredito  RELATOR final de cada célula
 *   fase 04 · redução   Polo V da rede: pontes, tensões, combinações, ajuste
 *                       conjunto BAO+SN; tudo determinístico
 *   fase 05 · parecer   um Sonnet redige o parecer da rede (mesma regra de ouro)
 */
const ORQ = (() => {
  const ouvintes = new Set();
  const historico = [];
  let ciclo = null, ultimo = null, seq = 0;
  function emitir(ev) {
    ev.ts = Date.now();
    ev.seq = ++seq;
    historico.push(ev);
    if (historico.length > 4000) historico.splice(0, historico.length - 4000);
    for (const f of ouvintes) { try { f(ev); } catch (e) { /* cliente caiu */ } }
  }
  function criarBarramento(contador) {
    return {
      enviar(tipo, de, para, payload) {
        const hex = URB1.enquadrar(tipo, JSON.stringify({ de, para, ...payload }));
        URB1.desenquadrar(hex); // confere o próprio quadro antes de soltar
        contador.quadros++;
        const chave = URB1.NOME_TIPO[tipo] + ' ' + de + '→' + para;
        contador.trafego[chave] = (contador.trafego[chave] || 0) + 1;
        emitir({ t: 'urb1', tipo: URB1.NOME_TIPO[tipo], de, para, bytes: hex.length / 2, hex: hex.length > 160 ? hex.slice(0, 160) + '…' : hex });
        return hex;
      },
      receber(hex) {
        try { return JSON.parse(URB1.desenquadrar(hex).payload); } catch (e) { contador.rejeitados++; emitir({ t: 'log', nivel: 'alerta', msg: 'quadro URB1 rejeitado: ' + e.message }); return null; }
      },
    };
  }
  const resumir = (t, n) => (t && t.length > n ? t.slice(0, n) + '\n…[cortado]' : t || '');
  /* Espera todas as tarefas; se uma falhar de forma fatal, aborta o ciclo e espera as
     outras encerrarem antes de propagar (senão células continuariam gastando API
     depois do "fim", e o próximo ciclo limparia o registro debaixo delas). */
  async function todas(c, promessas) {
    try { return await Promise.all(promessas); }
    catch (e) {
      c.fatal = c.fatal || e;
      // teto de gasto: não aborta (as outras células param sozinhas na próxima chamada); a redução, sem API, ainda roda
      if (!(e instanceof IA.ErroTeto) && !c.ctrl.signal.aborted) c.ctrl.abort();
      await Promise.allSettled(promessas);
      throw e;
    }
  }
  /* Fator de peso pela gravidade das violações: medida inventada (valor ± σ) = 1; número solto ou id inexistente = 0,5. */
  const fatorViolacoes = (s) => (s === 0 ? 1 : s <= 0.5 ? 0.7 : s < 2 ? 0.45 : s < 3 ? 0.2 : 0.1);
  const PSEUDO_REDE = { id: 'rede', n: 0, nome: 'REDE', telescopio: '17 células', banda: 'todas', sondas: 'todas', status: 'redução', produtos: [], tap: [], pipelines: [] };

  function novoEstado(cel) {
    return { id: cel.id, n: cel.n, nome: cel.nome, banda: cel.banda, status: cel.status, papeis: Object.fromEntries(PAPEIS.L.map((p) => [p.id, { estado: 'aguardando', texto: '', digest: [], guarda: null, passos: 0, ferramentas: [], erro: null, ms: 0 }])), execucoes: [], digest: [], peso: 1, motivosPeso: [], violacoes: 0, gravidade: 0, suprimida: false, numerosFerramentas: [] };
  }
  function montarUsuario(c, cel, papelId, extra = {}) {
    const st = c.celulas[cel.id];
    const partes = ['Ciclo ' + c.id + '. Pergunta: ' + c.objetivo];
    const ordem = ['coletor', 'analista', 'teorico', 'cetico', 'verificador', 'auditor', 'relator'];
    // EMBAIXADOR e RELATOR final leem o trabalho inteiro da célula (o RELATOR final inclui o próprio veredito anterior)
    const limite = papelId === 'embaixador' || (papelId === 'relator' && extra.final) ? ordem.length : ordem.indexOf(papelId);
    const anteriores = st && limite > 0 ? ordem.slice(0, limite).filter((p) => st.papeis[p].texto) : [];
    if (papelId === 'auditor') {
      partes.push('<<TEXTO_A_AUDITAR>>\n' + anteriores.map((p) => '### ' + p.toUpperCase() + '\n' + resumir(st.papeis[p].texto, 3500)).join('\n\n') + '\n<<FIM>>');
      partes.push('Guarda determinística por papel: ' + anteriores.map((p) => p + ': ' + (st.papeis[p].guarda ? st.papeis[p].guarda.violacoes.length + ' violação(ões)' + (st.papeis[p].guarda.violacoes.length ? ' ' + JSON.stringify(st.papeis[p].guarda.violacoes.slice(0, 5)) : '') : 'não rodou')).join('; '));
    } else if (anteriores.length) {
      partes.push('TRABALHO JÁ FEITO NESTA CÉLULA:\n' + anteriores.map((p) => '### ' + p.toUpperCase() + '\n' + resumir(st.papeis[p].texto, 2500)).join('\n\n'));
    }
    if (papelId === 'embaixador') {
      partes.push('SEU DIGEST ATUAL: ' + (st.digest.join(' | ') || '(vazio)'));
      const viz = (extra.caixa || []).map((hex) => c.barramento.receber(hex)).filter(Boolean);
      partes.push('VIZINHOS (rodada ' + extra.rodada + ', recebidos por URB1, peso de influência entre parênteses; não incorpore asserção de peso baixo sem razão própria):\n' + (viz.length ? viz.map((v) => v.de + ' (' + U.fmt(v.peso, 2) + '): ' + (v.digest || []).join(' | ')).join('\n') : '(nenhum vizinho ativo)'));
      if (extra.suprimidos && extra.suprimidos.length) partes.push('Suprimidos nesta rodada (peso abaixo do limiar): ' + extra.suprimidos.join(', '));
    }
    if (papelId === 'relator' && extra.final) {
      const emb = st.papeis.embaixador;
      partes.push('FASE FINAL. Gossip: ' + (emb.texto ? resumir(emb.texto, 2500) : '(esta célula não participou do gossip' + (st.suprimida ? ', peso ' + U.fmt(st.peso, 2) + ' abaixo do limiar' : '') + ')'));
      partes.push('Reescreva o veredito com o que a rede confirmou ou contradisse e publique o que ainda faltar.');
    }
    if (papelId === 'redutor') partes.push(extra.contexto || '');
    partes.push('Execute seu papel agora.');
    return partes.join('\n\n');
  }
  async function rodarPapel(c, cel, papelId, extra = {}) {
    const papel = papelId === 'redutor' ? REDUTOR : PAPEIS.porId[papelId];
    const st = cel.id === 'rede' ? c.rede : c.celulas[cel.id].papeis[papelId];
    Object.assign(st, { estado: 'ativo', erro: null });
    emitir({ t: 'papel', celula: cel.id, papel: papelId, estado: 'ativo', rodada: extra.rodada, final: !!extra.final });
    // números devolvidos por ferramentas nesta célula contam como fonte (são saída determinística, não do modelo)
    const doCel = cel.id === 'rede' ? (c.numerosRede = c.numerosRede || []) : c.celulas[cel.id].numerosFerramentas;
    const ctx = { celula: cel, papel: papelId, permitidas: papel.ferramentas, sinal: c.ctrl.signal, barramento: c.barramento, extrasGuarda: c.extrasGuarda, numerosCelula: doCel };
    const t0 = Date.now();
    try {
      const r = await IA.agente({
        sistema: PAPEIS.sistema(papel, cel, c), usuario: montarUsuario(c, cel, papelId, extra), ferramentas: papel.ferramentas, ctx,
        esforco: papel.esforcoCritico ? CFG.ESFORCO_CRITICO : CFG.ESFORCO, maxPassos: CFG.PASSOS_FERRAMENTA,
        aoPasso: (p) => {
          emitir({ t: 'ferramenta', celula: cel.id, papel: papelId, nome: p.nome, erro: p.erro });
          if (p.nome === 'publicar_medida' && !p.erro) emitir({ t: 'rede', tabela: POLOV.tabelaRede() });
        },
      });
      for (const v of r.numeros) if (doCel.length < 60000) doCel.push(v);
      const g = LEDGER.guarda(r.texto, FERR.escopoDe(cel.id, [...c.extrasGuarda, ...doCel]));
      Object.assign(st, { texto: r.texto, digest: IA.digestDe(r.texto), guarda: g, passos: r.passos, ferramentas: r.chamadas, stop: r.stop, ms: Date.now() - t0 });
      st.estado = g.violacoes.length ? 'violacao' : (r.stop === 'max_tokens' || r.stop === 'model_context_window_exceeded') ? 'truncado' : r.stop === 'refusal' ? 'recusa' : 'ok';
      if (cel.id !== 'rede') { c.celulas[cel.id].violacoes += g.violacoes.length; c.celulas[cel.id].gravidade += g.gravidade || 0; c.celulas[cel.id].execucoes.push({ papel: papelId, rodada: extra.rodada, final: !!extra.final, estado: st.estado, texto: r.texto, guarda: g, ms: st.ms }); }
      emitir({ t: 'papel', celula: cel.id, papel: papelId, estado: st.estado, digest: st.digest, violacoes: g.violacoes, conferidos: g.conferidos, passos: r.passos, ms: st.ms, rodada: extra.rodada, final: !!extra.final });
    } catch (e) {
      Object.assign(st, { estado: 'erro', erro: String(e && e.message || e), ms: Date.now() - t0 });
      emitir({ t: 'papel', celula: cel.id, papel: papelId, estado: 'erro', erro: st.erro });
      if (e instanceof IA.ErroTeto) throw e;
      if (e instanceof IA.ErroAPI && [401, 403, 404].includes(e.status)) throw e; // chave, permissão ou modelo: falha de todo o ciclo
      if (c.ctrl.signal.aborted) throw e;
    } finally {
      emitir({ t: 'custo', ...IA.custo });
    }
    return st;
  }
  function calcularPeso(c, cel) {
    const st = c.celulas[cel.id];
    const fase1 = ['coletor', 'analista', 'teorico', 'cetico', 'verificador', 'auditor', 'relator'];
    const ok = fase1.filter((p) => ['ok', 'violacao', 'truncado'].includes(st.papeis[p].estado)).length;
    const quorum = ok / fase1.length;
    const v = st.violacoes, s = st.gravidade;
    const fv = fatorViolacoes(s);
    const reprovado = /VEREDITO\s*:\s*\**\s*reprovado/i.test(st.papeis.auditor.texto || '');
    st.peso = quorum * fv * (reprovado ? 0.5 : 1);
    st.motivosPeso = [ok + '/7 papéis concluídos', v + ' violação(ões) da guarda' + (v ? ' (gravidade ' + U.fmt(s, 1) + ')' : ''), ...(reprovado ? ['AUDITOR reprovou'] : [])];
    st.suprimida = st.peso < CFG.LIMIAR_PESO;
    const fonte = ['relator', 'verificador', 'analista', 'coletor'].find((p) => st.papeis[p].digest.length && st.papeis[p].estado === 'ok');
    st.digest = fonte ? st.papeis[fonte].digest.slice() : [];
    emitir({ t: 'peso', celula: cel.id, peso: st.peso, motivos: st.motivosPeso, suprimida: st.suprimida });
  }
  /* Vizinhas: as duas do anel (garante que a informação circule pela rede
     inteira em rodadas sucessivas) + até 3 que publicaram a mesma grandeza. */
  function vizinhos(c, cel) {
    const pub = LEDGER.listar({ publicado: true });
    const minhas = new Set(pub.filter((e) => e.publicado_por === cel.id).map((e) => e.grandeza));
    const porGrandeza = c.lista.filter((o) => o.id !== cel.id && pub.some((e) => e.publicado_por === o.id && minhas.has(e.grandeza)));
    const i = c.lista.findIndex((o) => o.id === cel.id), n = c.lista.length;
    const anel = [c.lista[(i + 1) % n], c.lista[(i - 1 + n) % n]].filter((o) => o && o.id !== cel.id);
    const unicos = [...new Map(anel.map((o) => [o.id, o])).values()];
    const nAnel = unicos.length;
    for (const o of porGrandeza) { if (unicos.length >= nAnel + 3) break; if (!unicos.some((x) => x.id === o.id)) unicos.push(o); }
    return unicos;
  }
  async function gossip(c) {
    for (const cel of c.lista) {
      const st = c.celulas[cel.id];
      if (st.suprimida) { st.papeis.embaixador.estado = 'suprimido'; emitir({ t: 'papel', celula: cel.id, papel: 'embaixador', estado: 'suprimido' }); }
    }
    for (let r = 1; r <= c.rodadas; r++) {
      if (c.ctrl.signal.aborted) throw new Error('ciclo interrompido');
      c.fase = 'gossip ' + r;
      emitir({ t: 'ciclo', id: c.id, estado: 'executando', fase: c.fase });
      const ativas = c.lista.filter((cel) => !c.celulas[cel.id].suprimida);
      const antes = new Map(ativas.map((cel) => [cel.id, c.celulas[cel.id].digest.slice()]));
      const caixas = new Map(), suprimidos = new Map();
      for (const cel of ativas) for (const v of vizinhos(c, cel)) {
        const vs = c.celulas[v.id];
        if (vs.suprimida) { (suprimidos.get(cel.id) || suprimidos.set(cel.id, []).get(cel.id)).push(v.id); emitir({ t: 'log', nivel: 'alerta', msg: 'SUPRIMIDO ' + v.id + ' → ' + cel.id + ' · peso ' + U.fmt(vs.peso, 2) + ' abaixo do limiar' }); continue; }
        const hex = c.barramento.enviar(URB1.TIPO.GOSSIP, v.id, cel.id, { digest: vs.digest, peso: vs.peso, rodada: r });
        (caixas.get(cel.id) || caixas.set(cel.id, []).get(cel.id)).push(hex);
      }
      await todas(c, ativas.map((cel) => rodarPapel(c, cel, 'embaixador', { rodada: r, caixa: caixas.get(cel.id) || [], suprimidos: suprimidos.get(cel.id) || [] })));
      for (const cel of ativas) {
        const st = c.celulas[cel.id], e = st.papeis.embaixador;
        if (e.estado === 'ok' && e.digest.length) st.digest = e.digest.slice();
        if (e.estado === 'violacao') { st.peso *= fatorViolacoes(e.guarda ? e.guarda.gravidade : 1); st.motivosPeso.push('violação no gossip, rodada ' + r); st.suprimida = st.peso < CFG.LIMIAR_PESO; emitir({ t: 'peso', celula: cel.id, peso: st.peso, motivos: st.motivosPeso, suprimida: st.suprimida }); }
      }
      const deltas = ativas.map((cel) => 1 - U.jaccard(antes.get(cel.id), c.celulas[cel.id].digest));
      const delta = deltas.reduce((a, b) => a + b, 0) / (deltas.length || 1);
      c.gossip.push({ rodada: r, delta, ativas: ativas.length });
      emitir({ t: 'gossip', rodada: r, delta, ativas: ativas.length });
      if (delta < 0.12) break;
    }
  }
  async function reducao(c) {
    const regs = [];
    const tentar = (f) => { try { const r = f(); if (r) regs.push(...(Array.isArray(r) ? r : [r])); } catch (e) { emitir({ t: 'log', nivel: 'info', msg: 'Polo V: ' + e.message }); } };
    tentar(() => POLOV.identidades('rede'));
    const pub = () => LEDGER.listar({ publicado: true });
    const temCalc = (cel) => pub().some((e) => e.publicado_por === cel && e.tag === 'C');
    if (temCalc('desi') && temCalc('des')) {
      for (const modelo of ['lcdm', 'w0wacdm']) {
        try {
          emitir({ t: 'log', nivel: 'info', msg: 'rede: ajuste conjunto DESI DR2 BAO + DES-Dovekie, ' + modelo });
          const res = await PIPE.ajustar(['bao', 'sn'], modelo, { sinal: c.ctrl.signal });
          for (const e of PIPE.registrar('rede', res, [])) { if (e.sigma > 0 || e.sigma_mais > 0) { LEDGER.publicar(e.id, 'rede'); regs.push(e); } }
        } catch (e) { emitir({ t: 'log', nivel: 'alerta', msg: 'ajuste conjunto falhou: ' + e.message }); }
      }
    }
    const hrd = pub().find((e) => e.grandeza === 'hrd' && e.publicado_por === 'desi');
    const h0Escada = pub().find((e) => e.grandeza === 'H0' && e.publicado_por === 'hubble' && e.tag === 'C');
    const omb = LEDGER.listar({ grandeza: 'ombh2' }).find((e) => e.celula === 'planck'), omc = LEDGER.listar({ grandeza: 'omch2' }).find((e) => e.celula === 'planck');
    if (hrd && h0Escada) tentar(() => POLOV.rdTardio(hrd.id, h0Escada.id));
    if (omb && omc) {
      let rd = null;
      tentar(() => (rd = POLOV.rdCMB(omb.id, omc.id)));
      if (rd && hrd) tentar(() => POLOV.h0ReguaInversa(hrd.id, rd.id));
    }
    for (const e of regs) if ((e.grandeza === 'rd' || e.grandeza === 'H0') && e.celula === 'rede' && !e.publicado) { try { LEDGER.publicar(e.id, 'rede'); } catch (x) { /* sem σ */ } }
    const rdT = regs.find((e) => e.grandeza === 'rd' && /tardio/.test(e.rotulo)), rdC = regs.find((e) => e.grandeza === 'rd' && /CMB/.test(e.rotulo));
    if (rdT && rdC) tentar(() => POLOV.tensao(rdT.id, rdC.id));
    const tab = POLOV.tabelaRede();
    for (const [g, t] of Object.entries(tab)) {
      for (const p of t.pares) if (p.independentes && p.sigma >= CFG.LIMIAR_TENSAO) tentar(() => POLOV.tensao(p.a, p.b));
      // combinação: medidas independentes e do mesmo modelo, consistentes entre si
      const grupos = {};
      for (const m of t.medidas) (grupos[m.modelo] = grupos[m.modelo] || []).push(m);
      for (const ms of Object.values(grupos)) {
        const escolhidas = [];
        for (const m of ms) if (escolhidas.every((o) => !POLOV.sobrepoe(LEDGER.obter(o), LEDGER.obter(m.id)).length)) escolhidas.push(m.id);
        if (escolhidas.length >= 2) tentar(() => POLOV.combinar(escolhidas));
      }
    }
    c.tabela = POLOV.tabelaRede();
    emitir({ t: 'rede', tabela: c.tabela, registros: regs.map(FERR.compacto) });
    return regs;
  }
  const REDUTOR = { id: 'redutor', nome: 'REDUTOR', sigla: 'RED', ferramentas: ['rede', 'ledger', 'literatura'], esforcoCritico: true,
    missao: 'Redija o parecer da rede sobre matéria escura e energia escura a partir da tabela da rede, dos registros da redução (Polo V) e dos vereditos das células com peso acima do limiar. Separe o que é medido aqui [C], o que é reportado [D] e o que é lacuna [L]; diga onde há tensão e se as medidas comparadas são independentes.',
    formato: 'SÍNTESE · TENSÕES · O QUE A REDE NÃO CONSEGUE DIZER · PRÓXIMOS DADOS (só fatos do registro).' };

  async function executar(op) {
    if (ciclo && ciclo.estado === 'executando') throw new Error('já há um ciclo em execução: ' + ciclo.id);
    const ids = (op.celulas && op.celulas.length ? op.celulas : CELULAS.lista.map((c) => c.id)).filter((id) => Object.hasOwn(CELULAS.porId, id));
    const c = {
      id: 'CIC-' + new Date().toISOString().replace(/[-:T]/g, '').slice(0, 14) + '-' + U.sha256(String(Math.random())).slice(0, 4),
      objetivo: String(op.objetivo || 'Medir o que for possível sobre matéria escura e energia escura com os dados públicos de cada célula e cruzar entre células.').slice(0, 2000),
      lista: ids.map((id) => CELULAS.porId[id]), rodadas: Math.max(0, Math.min(6, Number(op.rodadas ?? CFG.RODADAS_MAX))),
      parecer: op.parecer !== false, estado: 'executando', fase: 'local', inicio: Date.now(), ctrl: new AbortController(),
      contador: { quadros: 0, rejeitados: 0, trafego: {} }, gossip: [], celulas: {}, rede: { estado: 'aguardando', texto: '' }, modo: CFG.MODO,
    };
    c.barramento = criarBarramento(c.contador);
    c.extrasGuarda = LEDGER.extrairNumeros(c.objetivo).flatMap((x) => x.valores);
    const mv = /-(\d+)-(\d+)$/.exec(CFG.MODELO); if (mv) c.extrasGuarda.push(Number(mv[1] + '.' + mv[2]));
    for (const cel of c.lista) c.celulas[cel.id] = novoEstado(cel);
    // registro novo por ciclo: literatura e fatos voltam; cálculos e publicações recomeçam
    LEDGER.limpar();
    LIT.carregar();
    FERR.tabelas.clear();
    ciclo = c;
    IA.zerarCusto();
    emitir({ t: 'ciclo', id: c.id, estado: 'executando', fase: 'local', objetivo: c.objetivo, celulas: ids, modo: CFG.MODO, rodadas: c.rodadas });
    (async () => {
      try {
        try {
          await todas(c, c.lista.map(async (cel) => {
            await rodarPapel(c, cel, 'coletor');
            await rodarPapel(c, cel, 'analista');
            await todas(c, [rodarPapel(c, cel, 'teorico'), rodarPapel(c, cel, 'cetico')]);
            await rodarPapel(c, cel, 'verificador');
            await rodarPapel(c, cel, 'auditor');
            await rodarPapel(c, cel, 'relator');
            calcularPeso(c, cel);
          }));
          if (c.rodadas > 0) await gossip(c);
          c.fase = 'veredito';
          emitir({ t: 'ciclo', id: c.id, estado: 'executando', fase: c.fase });
          await todas(c, c.lista.map((cel) => rodarPapel(c, cel, 'relator', { final: true })));
        } catch (e) {
          if (!(e instanceof IA.ErroTeto)) throw e;
          // teto de gasto: o que resta sem API (pesos, redução) ainda sai; o parecer não é redigido
          c.tetoAtingido = c.fase;
          c.aviso = 'teto de gasto (US$ ' + CFG.TETO_USD + ') atingido na fase "' + c.fase + '": gossip, veredito final e parecer ficaram incompletos; a redução determinística foi feita com o que havia';
          emitir({ t: 'log', nivel: 'alerta', msg: c.aviso });
          for (const cel of c.lista) if (!c.celulas[cel.id].motivosPeso.length) calcularPeso(c, cel);
        }
        c.fase = 'redução';
        emitir({ t: 'ciclo', id: c.id, estado: 'executando', fase: c.fase });
        const regs = await reducao(c);
        if (c.parecer && !c.tetoAtingido) {
          c.fase = 'parecer';
          emitir({ t: 'ciclo', id: c.id, estado: 'executando', fase: c.fase });
          const vereditos = c.lista.filter((cel) => !c.celulas[cel.id].suprimida).map((cel) => cel.nome + ' (peso ' + U.fmt(c.celulas[cel.id].peso, 2) + '): ' + resumir(c.celulas[cel.id].papeis.relator.texto, 1200)).join('\n\n');
          const suprimidas = c.lista.filter((cel) => c.celulas[cel.id].suprimida).map((cel) => cel.nome + ' (peso ' + U.fmt(c.celulas[cel.id].peso, 2) + ')');
          await rodarPapel(c, PSEUDO_REDE, 'redutor', { contexto: 'REGISTROS DA REDUÇÃO (Polo V):\n' + U.json(regs.map(FERR.compacto), 9000) + '\n\nVEREDITOS DAS CÉLULAS ATIVAS:\n' + vereditos + (suprimidas.length ? '\n\nCÉLULAS SUPRIMIDAS (não use o texto delas): ' + suprimidas.join(', ') : '') });
        }
        c.estado = 'concluido';
      } catch (e) {
        const causa = c.fatal || e;
        c.estado = c.parado ? 'interrompido' : 'erro';
        c.erro = String(causa && causa.message || causa);
        emitir({ t: 'log', nivel: 'erro', msg: 'ciclo ' + c.estado + ': ' + c.erro });
      } finally {
        c.fim = Date.now();
        c.arquivo = salvar(c);
        ultimo = resumo(c);
        emitir({ t: 'fim', id: c.id, estado: c.estado, erro: c.erro, aviso: c.aviso, duracao_s: Math.round((c.fim - c.inicio) / 1000), custo: { ...IA.custo }, arquivo: c.arquivo });
      }
    })();
    return { id: c.id, celulas: ids, rodadas: c.rodadas, modo: CFG.MODO };
  }
  function resumo(c) {
    if (!c) return null;
    return {
      id: c.id, objetivo: c.objetivo, estado: c.estado, fase: c.fase, modo: c.modo, erro: c.erro, aviso: c.aviso, tetoAtingido: c.tetoAtingido || null, inicio: c.inicio, fim: c.fim, rodadas: c.rodadas,
      gossip: c.gossip, urb1: c.contador, custo: { ...IA.custo },
      celulas: Object.fromEntries(Object.entries(c.celulas).map(([id, s]) => [id, { id, n: s.n, nome: s.nome, banda: s.banda, status: s.status, peso: s.peso, suprimida: s.suprimida, violacoes: s.violacoes, motivosPeso: s.motivosPeso, digest: s.digest, papeis: Object.fromEntries(Object.entries(s.papeis).map(([p, v]) => [p, { estado: v.estado, passos: v.passos, ms: v.ms, erro: v.erro, violacoes: v.guarda ? v.guarda.violacoes.length : 0, digest: v.digest }])) }])),
      rede: { estado: c.rede.estado, texto: c.rede.texto, guarda: c.rede.guarda }, tabela: c.tabela || (c.estado === 'executando' ? POLOV.tabelaRede() : null),
    };
  }
  function salvar(c) {
    try {
      const dir = path.join(CFG.DATA_DIR, 'dossies');
      fs.mkdirSync(dir, { recursive: true });
      const arq = path.join(dir, c.id + '.json');
      const celulasCompletas = Object.fromEntries(Object.entries(c.celulas).map(([id, s]) => [id, { ...s, numerosFerramentas: undefined }]));
      const dossie = { ...resumo(c), versao: VERSAO, modelo: CFG.MODELO, celulasCompletas, registros: LEDGER.listar({}).map((e) => ({ ...e, numeros_guarda: undefined })) };
      fs.writeFileSync(arq, JSON.stringify(dossie));
      return arq;
    } catch (e) { emitir({ t: 'log', nivel: 'alerta', msg: 'dossiê não salvo: ' + e.message }); return null; }
  }
  function parar() { if (ciclo && ciclo.estado === 'executando') { ciclo.parado = true; ciclo.ctrl.abort(); return true; } return false; }
  const estado = () => ({ versao: VERSAO, modo: CFG.MODO, modelo: CFG.MODELO, seq, ciclo: resumo(ciclo) || ultimo, celulas: CELULAS.lista, limitador: IA.limitar.estado(), custo: { ...IA.custo }, teto: CFG.TETO_USD, limiarPeso: CFG.LIMIAR_PESO, limiarTensao: CFG.LIMIAR_TENSAO });
  const detalheCelula = (id) => (ciclo && ciclo.celulas[id] ? { ...ciclo.celulas[id], numerosFerramentas: undefined } : null);
  return { emitir, ouvintes, historico, executar, parar, estado, detalheCelula, atual: () => ciclo, REDUTOR, PSEUDO_REDE };
})();

/* ── 17a · imagem de fundo (campo profundo, WebP enviado pelo usuário em 22/out/2026; 1450×816, 61288 bytes) ─ */
const FUNDO = (() => {
  const b64 = [
    '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',
  ].join('');
  const buf = Buffer.from(b64, 'base64');
  return { tipo: 'image/webp', buf, etag: '"' + U.sha256(buf).slice(0, 16) + '"', bytes: buf.length };
})();

/* ── 17 · painel na gramática do UROBOROS ───────────────────────────────────
 * Tela de entrada (boot) → quadro geral (deck: topbar / grade de células /
 * barramento) → visão focada da célula (8 papéis) → síntese (mapa de tráfego
 * URB1 + parecer + rede + Polo V). Fundo: campo profundo (/fundo.webp) sob um
 * véu escuro; os painéis são translúcidos. Um arquivo, sem dependências.
 */
const PAINEL = (() => {
  function html(nonce, exigeToken) {
    return `<!doctype html>
<html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>PARALLAX · Células</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@500;600;700&family=JetBrains+Mono:wght@300;400;500;700&display=swap" rel="stylesheet">
<link rel="preload" as="image" href="/fundo.webp">
<style>
:root{
  --ink:#05070b; --panel:#0a0f16; --panel2:#070b11; --line:#16202c; --line2:#101922;
  --teal:#35e0c0; --teal-dim:#12594e; --amber:#ffb44a; --amber-dim:#5a3d14;
  --txt:#8ea3b8; --txt-hi:#dce9f2; --txt-lo:#4c5f72; --red:#ff5f56;
  --mono:'JetBrains Mono',ui-monospace,'SF Mono',Menlo,Consolas,monospace;
  --disp:'Barlow Condensed','Arial Narrow',sans-serif;
  --veu:.42;
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%}
body{background:var(--ink);color:var(--txt);font-family:var(--mono);font-size:13px;overflow:hidden;-webkit-font-smoothing:antialiased;-webkit-text-size-adjust:100%}
/* o campo profundo por trás de tudo: ambiente, nunca conteúdo */
#fundo{position:fixed;inset:0;z-index:0;pointer-events:none;background:#000 url(/fundo.webp) center/cover no-repeat}
#fundo::after{content:'';position:absolute;inset:0;background:
  radial-gradient(120% 90% at 50% 50%,rgba(5,7,11,var(--veu)),rgba(5,7,11,calc(var(--veu) + .25)) 100%),
  linear-gradient(180deg,rgba(5,7,11,.35),transparent 30%,transparent 70%,rgba(5,7,11,.55))}
body.deck-on{--veu:.58}
.eyebrow{font-family:var(--disp);text-transform:uppercase;letter-spacing:.22em;font-weight:600}
button{font-family:var(--mono);cursor:pointer;border:none;background:none;color:inherit}
input,select,textarea{font-family:var(--mono);color:inherit}
:focus-visible{outline:1px solid var(--teal);outline-offset:2px}
::selection{background:rgba(53,224,192,.28)}
.oculto{display:none!important}

/* ───────────────────────────── tela de entrada ──────────────────────────── */
#boot{position:fixed;inset:0;z-index:40;display:grid;place-items:center;padding:20px;overflow:auto}
.boot-card{width:min(820px,100%);max-height:100%;overflow:auto;background:rgba(5,7,11,.66);backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);border:1px solid var(--line);border-radius:3px;padding:26px 24px 22px}
.boot-mark{display:flex;align-items:baseline;gap:14px;margin-bottom:6px;flex-wrap:wrap}
.boot-mark h1{font-family:var(--disp);font-size:clamp(38px,8vw,72px);font-weight:700;letter-spacing:.16em;color:var(--txt-hi);line-height:.9}
.boot-mark span{font-size:10px;letter-spacing:.3em;color:var(--teal)}
.boot-sub{font-size:11px;color:var(--txt-lo);letter-spacing:.08em;margin-bottom:22px;line-height:1.7}
.boot-sub b{color:var(--amber);font-weight:500}
.field-label{display:flex;justify-content:space-between;align-items:center;font-size:10px;letter-spacing:.24em;color:var(--txt-lo);margin-bottom:8px}
#input{width:100%;min-height:110px;resize:vertical;background:#000;color:var(--txt-hi);border:1px solid var(--line);border-radius:2px;padding:14px 16px;font-size:14px;line-height:1.65;caret-color:var(--teal)}
#input:focus{outline:none;border-color:var(--teal-dim);box-shadow:0 0 0 1px #35e0c022,0 0 40px -18px var(--teal)}
#input::placeholder{color:#2c3a48}
.hexmirror{margin-top:10px;border:1px solid var(--line2);background:#04060a;padding:9px 11px;border-radius:2px}
.hexmirror .cap{font-size:9px;letter-spacing:.24em;color:var(--txt-lo);margin-bottom:5px}
.hexmirror code{display:block;font-size:10px;line-height:1.55;color:var(--teal);word-break:break-all;max-height:52px;overflow:hidden;opacity:.85}
.hexmirror code em{color:var(--amber);font-style:normal}
.opts{display:flex;flex-wrap:wrap;gap:10px;margin-top:16px}
.opt{border:1px solid var(--line);border-radius:2px;padding:8px 12px;font-size:10px;letter-spacing:.14em;color:var(--txt-lo);display:flex;align-items:center;gap:9px;background:var(--panel2)}
.opt b{color:var(--txt-hi);font-weight:500}
.opt input[type=checkbox]{accent-color:var(--teal)}
.opt select,.opt input[type=password]{background:#000;border:1px solid var(--line);color:var(--teal);font-size:11px;padding:4px 6px;border-radius:2px}
.opt input[type=password]{width:170px;letter-spacing:.1em}
.cels{display:flex;flex-wrap:wrap;gap:6px;margin-top:14px}
.cel-chip{border:1px solid var(--line);border-radius:2px;padding:6px 10px;font-size:10px;letter-spacing:.12em;color:var(--txt-lo);cursor:pointer;background:var(--panel2);user-select:none;display:flex;gap:7px;align-items:center}
.cel-chip b{color:var(--txt-hi);font-weight:500;letter-spacing:.18em}
.cel-chip.on{border-color:var(--teal-dim);color:var(--teal)}
.cel-chip.on b{color:var(--teal)}
#launch{margin-top:20px;width:100%;padding:15px;background:var(--teal);color:#01120e;border-radius:2px;font-size:12px;font-weight:700;letter-spacing:.28em;transition:.18s}
#launch:hover{background:#5cf0d2;box-shadow:0 0 50px -12px var(--teal)}
#launch:disabled{background:var(--line);color:var(--txt-lo);cursor:not-allowed;box-shadow:none}
.ghosts{display:flex;gap:10px;margin-top:10px;flex-wrap:wrap}
.ghost{border:1px solid var(--line);padding:8px 12px;font-size:10px;letter-spacing:.18em;color:var(--txt);border-radius:2px;background:rgba(4,6,10,.5)}
.ghost:hover{border-color:var(--teal-dim);color:var(--teal)}
.ghost:disabled{opacity:.4;cursor:not-allowed}
.boot-note{margin-top:12px;font-size:10px;color:var(--txt-lo);line-height:1.7}
.boot-note b{color:var(--txt);font-weight:500}
#health{color:var(--txt)}
#health.err{color:var(--red)}
table.t{width:100%;border-collapse:collapse;font-size:10px;margin-top:8px}
table.t th,table.t td{text-align:left;padding:5px 6px;border-bottom:1px solid var(--line2);vertical-align:top}
table.t th{color:var(--txt-lo);font-weight:500;letter-spacing:.14em;font-size:9px}
td.ok{color:var(--teal)} td.falha{color:var(--red)} td.num{white-space:nowrap;color:var(--txt-hi)}
.dossies{margin-top:18px;border-top:1px solid var(--line2);padding-top:12px}
.dossie{display:flex;gap:12px;align-items:center;padding:6px 8px;border:1px solid transparent;border-radius:2px;cursor:pointer;font-size:10px;color:var(--txt-lo);letter-spacing:.08em}
.dossie:hover{border-color:var(--line);color:var(--txt)}
.dossie b{color:var(--txt-hi);font-weight:500;letter-spacing:.14em}
.credito{margin-top:14px;font-size:9px;color:#2a3846;letter-spacing:.08em}

/* ────────────────────────────────── quadro geral ─────────────────────────── */
#deck{position:fixed;inset:0;z-index:1;display:none;grid-template-rows:auto 1fr auto;height:100%}
#deck.on{display:grid}
.topbar{display:flex;align-items:center;gap:14px;padding:10px 14px;border-bottom:1px solid var(--line);background:#04060acc;backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px);flex-wrap:wrap}
.topbar h2{font-family:var(--disp);font-size:17px;letter-spacing:.22em;color:var(--txt-hi);font-weight:700}
.phase{font-size:10px;letter-spacing:.2em;color:var(--teal);border:1px solid var(--teal-dim);padding:3px 8px;border-radius:2px;background:#35e0c00d;white-space:nowrap}
.phase.err{color:var(--red);border-color:#5a2420;background:#ff5f560d}
.phase.warn{color:var(--amber);border-color:var(--amber-dim);background:#ffb44a0d}
.stats{margin-left:auto;display:flex;gap:14px;font-size:10px;color:var(--txt-lo);letter-spacing:.1em;flex-wrap:wrap}
.stats b{color:var(--txt-hi);font-weight:500}
#grid{overflow:auto;padding:14px;display:grid;gap:12px;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));align-content:start}
.cl{border:1px solid var(--line);border-radius:3px;background:linear-gradient(180deg,rgba(10,15,22,.8),rgba(7,11,17,.86));backdrop-filter:blur(6px);-webkit-backdrop-filter:blur(6px);overflow:hidden;cursor:pointer;transition:border-color .18s,transform .18s;display:flex;flex-direction:column;min-height:210px;position:relative}
.cl:hover{border-color:#243444;transform:translateY(-2px)}
.cl.busy{border-color:var(--teal-dim);box-shadow:0 0 40px -22px var(--teal)}
.cl.done{border-color:#1d3b4a}
.cl.fail{border-color:#4a1d1d}
.cl.sup{border-style:dashed;opacity:.62}
.cl-head{display:flex;align-items:center;gap:8px;padding:7px 10px;border-bottom:1px solid var(--line2);background:#060a0fcc}
.dot{width:5px;height:5px;border-radius:50%;background:var(--txt-lo);flex:none}
.cl.busy .dot{background:var(--teal);animation:pulse 1s infinite}
.cl.done .dot{background:var(--teal)}
.cl.fail .dot{background:var(--red)}
@keyframes pulse{50%{opacity:.15}}
.cl-id{font-size:10px;letter-spacing:.2em;color:var(--txt-hi)}
.cl-title{font-family:var(--disp);font-size:12px;letter-spacing:.12em;text-transform:uppercase;color:var(--txt);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex:0 0 auto;max-width:55%}
.cl-stat{margin-left:auto;font-size:9px;letter-spacing:.14em;color:var(--txt-lo);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0;flex:1 1 0;text-align:right}
.cl-body{display:grid;grid-template-columns:116px 1fr;flex:1;min-height:0}
.papeis{display:grid;grid-template-columns:1fr 1fr;gap:4px;padding:8px;border-right:1px solid var(--line2);align-content:start}
.pp{font-size:8px;letter-spacing:.16em;text-align:center;padding:5px 0;border:1px solid var(--line2);border-radius:2px;color:var(--txt-lo);background:#04060a99}
.pp.ativo{color:#01120e;background:var(--teal);border-color:var(--teal);animation:pulse 1.1s infinite}
.pp.ok{color:var(--teal);border-color:var(--teal-dim)}
.pp.violacao{color:var(--red);border-color:#5a2420}
.pp.erro{color:#01120e;background:var(--red);border-color:var(--red)}
.pp.truncado,.pp.recusa{color:var(--amber);border-color:var(--amber-dim)}
.pp.suprimido{color:var(--red);border-style:dashed;border-color:#5a2420;opacity:.7}
.pane{padding:8px 9px;overflow:hidden;display:flex;flex-direction:column;gap:6px;min-width:0}
.pane-h{display:flex;align-items:baseline;gap:6px;font-size:8px;letter-spacing:.2em}
.pane-h .n{color:var(--txt-lo)}.pane-h .t{color:var(--teal)}.pane-h .m{margin-left:auto;color:#2f3f4e;font-size:8px}
.pane-txt{font-size:10px;line-height:1.5;color:#7fd8c6;overflow:hidden;white-space:pre-wrap;word-break:break-word;flex:1;-webkit-mask-image:linear-gradient(180deg,#000 60%,transparent);mask-image:linear-gradient(180deg,#000 60%,transparent)}
.pane-txt.vazio{color:#2a3846}
.pane-hex{font-size:8px;line-height:1.4;color:#1f4d45;word-break:break-all;max-height:26px;overflow:hidden}
.peso{height:3px;background:var(--line2)}
.peso i{display:block;height:100%;background:var(--teal)}
.cl.sup .peso i{background:var(--red)}
.bus{border-top:1px solid var(--line);background:#04060acc;backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px);padding:6px 12px;display:flex;align-items:center;gap:12px;overflow:hidden}
.bus .tag{font-size:9px;letter-spacing:.2em;color:var(--txt-lo);flex:none}
#busline{font-size:9px;color:#1d4f47;white-space:nowrap;overflow:hidden;flex:1;text-overflow:ellipsis}
#busline.a{color:var(--amber)}#busline.e{color:var(--red)}

/* ─────────────────────────────── visão focada ───────────────────────────── */
#focus{position:fixed;inset:0;z-index:30;background:#04060af2;backdrop-filter:blur(4px);-webkit-backdrop-filter:blur(4px);display:none;flex-direction:column}
#focus.on{display:flex}
.f-head{display:flex;align-items:center;gap:12px;padding:12px 14px;border-bottom:1px solid var(--line);flex-wrap:wrap}
.f-head h3{font-family:var(--disp);font-size:19px;letter-spacing:.2em;color:var(--txt-hi);text-transform:uppercase}
.f-sub{font-size:9px;color:var(--txt-lo);letter-spacing:.12em}
.f-body{flex:1;overflow:auto;display:grid;grid-template-columns:1fr 1fr;gap:12px;padding:14px;align-content:start}
@media(max-width:760px){.f-body{grid-template-columns:1fr}}
.f-pane{border:1px solid var(--line);background:var(--panel);border-radius:3px;padding:12px 14px;min-width:0}
.f-pane h4{font-size:10px;letter-spacing:.24em;color:var(--teal);margin-bottom:6px}
.f-pane.i h4{color:var(--amber)}
.f-meta{font-size:9px;color:var(--txt-lo);letter-spacing:.12em;margin-bottom:10px}
.f-txt{font-size:12px;line-height:1.7;white-space:pre-wrap;word-break:break-word;color:var(--txt-hi)}
.f-viol{margin-top:10px;border-top:1px solid var(--line2);padding-top:8px;font-size:10px;line-height:1.6;color:var(--red)}
.f-erro{font-size:11px;color:var(--red)}

/* ───────────────────────────── síntese + topologia ──────────────────────── */
#final{position:fixed;inset:0;z-index:35;background:#04060af7;backdrop-filter:blur(6px);-webkit-backdrop-filter:blur(6px);display:none;overflow:auto}
#final.on{display:block}
.f-wrap{max-width:1180px;margin:0 auto;padding:22px 18px 60px;display:grid;gap:20px;grid-template-columns:300px 1fr}
@media(max-width:860px){.f-wrap{grid-template-columns:1fr}}
.map-card{border:1px solid var(--line);background:var(--panel);border-radius:3px;padding:14px;align-self:start;position:sticky;top:22px}
@media(max-width:860px){.map-card{position:static}}
.map-card .cap{font-size:9px;letter-spacing:.24em;color:var(--txt-lo);margin-bottom:10px}
svg.map{width:100%;height:auto;display:block}
.legend{display:flex;gap:12px;font-size:9px;letter-spacing:.1em;color:var(--txt-lo);margin-top:12px;flex-wrap:wrap}
.legend i{width:8px;height:2px;display:inline-block;vertical-align:middle;margin-right:5px}
.kv{margin-top:12px;font-size:9.5px;line-height:1.9;color:var(--txt-lo);border-top:1px solid var(--line2);padding-top:10px}
.kv b{color:var(--txt-hi);font-weight:500}
.md{font-size:13px;line-height:1.8;color:var(--txt-hi);min-width:0}
.md h2{font-family:var(--disp);font-size:14px;letter-spacing:.22em;text-transform:uppercase;color:var(--teal);margin:22px 0 8px;font-weight:600}
.md h2:first-child{margin-top:0}
.md pre{white-space:pre-wrap;word-break:break-word;font:inherit;font-size:12.5px;line-height:1.75}
.md .vazio{color:var(--txt-lo)}
.md h3{font-size:10px;letter-spacing:.2em;color:var(--txt);margin:14px 0 4px;font-weight:500}
.md .tens{color:var(--red)}.md .dep{color:var(--txt-lo)}.md .avisos{color:var(--amber);font-size:10px}
.tab-wrap{overflow:auto}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
@media(max-width:520px){.boot-card{padding:18px 14px}.topbar{padding:8px 10px;gap:8px}.stats{gap:8px}#grid{padding:10px;grid-template-columns:1fr}.cl-body{grid-template-columns:104px 1fr}}
</style></head><body>
<div id="fundo" aria-hidden="true"></div>

<!-- ══════════════════════════ TELA DE ENTRADA ══════════════════════════ -->
<section id="boot">
  <div class="boot-card">
    <div class="boot-mark"><h1>PARALLAX</h1><span class="eyebrow" id="marca-sub">17 células · 8 × Claude Sonnet</span></div>
    <p class="boot-sub">Barramento hexadecimal <b>URB1</b> · uma célula por telescópio · <b>matéria escura</b> × <b>energia escura</b> · Polo V determinístico · gossip com supressão por peso</p>

    <div class="field-label"><span class="eyebrow">Pergunta do ciclo</span><span id="charc">0 / 2000</span></div>
    <textarea id="input" spellcheck="false" maxlength="2000" placeholder="O que as células devem medir e cruzar neste ciclo.">Medir o que for possível sobre matéria escura e energia escura com os dados públicos de cada célula e cruzar entre células.</textarea>

    <div class="hexmirror">
      <div class="cap eyebrow">Quadro na saída do barramento</div>
      <code id="mirror">55524231 01 01 0000 <em>—</em></code>
    </div>

    <div class="cels" id="cels"></div>

    <div class="opts">
      <label class="opt"><span>RODADAS DE <b>GOSSIP</b></span>
        <select id="rod"><option>0</option><option>1</option><option>2</option><option selected>3</option><option>4</option><option>5</option><option>6</option></select>
      </label>
      <label class="opt"><input type="checkbox" id="par" checked><span><b>PARECER</b> DA REDE</span></label>
      ${exigeToken ? '<label class="opt"><span>TOKEN</span><input type="password" id="tok" placeholder="ACCESS_TOKEN" autocomplete="off"></label>' : '<input type="hidden" id="tok" value="">'}
    </div>

    <button id="launch">INICIAR CICLO</button>
    <div class="ghosts"><button class="ghost" id="bTes">TESTAR CONEXÕES</button><button class="ghost" id="bCurto" title="DESI, DES, Hubble e Planck · 1 rodada">CICLO DE TESTE</button><button class="ghost" id="bDeck">VER QUADRO</button></div>
    <p class="boot-note">Primeira vez: <b>testar conexões</b> confere chave, modelo, serviços TAP e arquivos de dados; <b>ciclo de teste</b> roda 4 células com 1 rodada para medir custo e tempo reais. Cada papel só escreve números que vieram de ferramenta ou do registro; a guarda confere e a rede suprime quem inventa.</p>
    <p class="boot-note" id="health">ligando…</p>
    <div id="testes"></div>
    <div class="dossies" id="dossies"></div>
    <p class="credito">Imagem de fundo: campo profundo do Webb (NASA, ESA, CSA, STScI)</p>
  </div>
</section>

<!-- ═══════════════════════════ GRADE DE CÉLULAS ════════════════════════ -->
<main id="deck">
  <header class="topbar">
    <h2>PARALLAX</h2>
    <span class="phase" id="phase">EM ESPERA</span>
    <div class="stats">
      <span>CÉLULAS <b id="s-cel">0</b></span>
      <span>QUADROS <b id="s-frames">0</b></span>
      <span>CHAMADAS <b id="s-call">0</b></span>
      <span>CUSTO <b id="s-usd">—</b></span>
      <span>TEMPO <b id="s-time">0s</b></span>
    </div>
    <button class="ghost" id="btn-final">SÍNTESE</button>
    <button class="ghost" id="btn-stop" disabled>PARAR</button>
    <button class="ghost" id="btn-reset">NOVO CICLO</button>
  </header>
  <div id="grid"></div>
  <footer class="bus"><span class="tag eyebrow">URB1</span><div id="busline">barramento ocioso</div></footer>
</main>

<!-- ════════════════════════════ VISÃO FOCADA ══════════════════════════ -->
<section id="focus">
  <header class="f-head">
    <h3 id="f-title">C00</h3>
    <span class="phase" id="f-stat">—</span>
    <span class="f-sub" id="f-sub"></span>
    <button class="ghost" style="margin-left:auto" id="f-prev">◀</button><button class="ghost" id="f-next">▶</button><button class="ghost" id="f-close">FECHAR ✕</button>
  </header>
  <div class="f-body" id="f-body"></div>
</section>

<!-- ═══════════════════════════ SÍNTESE FINAL ══════════════════════════ -->
<section id="final">
  <div class="f-wrap">
    <aside class="map-card">
      <div class="cap eyebrow">Topologia · tráfego URB1</div>
      <svg class="map" id="map" viewBox="0 0 300 300" aria-label="mapa de tráfego entre células"></svg>
      <div class="legend">
        <span><i style="background:var(--teal)"></i>gossip</span>
        <span><i style="background:var(--amber)"></i>medida publicada</span>
        <span><i style="background:var(--red)"></i>suprimida</span>
      </div>
      <div class="kv" id="kv"></div>
      <button class="ghost" id="fin-close" style="width:100%;margin-top:14px">VOLTAR À GRADE</button>
    </aside>
    <article class="md" id="reduce"><p class="vazio">A síntese aparece aqui quando a redução e o parecer concluírem.</p></article>
  </div>
</section>

<script nonce="${nonce}">
(function(){
'use strict';
var PAP=[['coletor','COL'],['analista','ANA'],['teorico','TEO'],['cetico','CET'],['verificador','VER'],['auditor','AUD'],['embaixador','EMB'],['relator','REL']];
var NOME_PAP={coletor:'COLETOR',analista:'ANALISTA',teorico:'TEÓRICO',cetico:'CÉTICO',verificador:'VERIFICADOR',auditor:'AUDITOR',embaixador:'EMBAIXADOR',relator:'RELATOR'};
var ADVERSO={cetico:1,auditor:1};
var FASE={local:'01 LOCAL',veredito:'03 VEREDITO','redução':'04 REDUÇÃO',parecer:'05 PARECER'};
var est={lista:[],porId:{},sel:{},ciclo:null,celulas:{},ordem:[],urb:0,trafego:{},chamadas:0,custo:null,tabela:null,registros:[],parecer:'',inicio:0,modo:'',teto:0,limiarPeso:.5,limiarTensao:3,foco:null,timer:null,fonte:'vivo'};
function $(i){return document.getElementById(i)}
function esc(s){return String(s==null?'':s).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]})}
function casas(s){if(!(s>0))return 4;return Math.max(0,Math.min(8,Math.ceil(-Math.log10(s))+1))}
function fmt(x,c){if(x==null||!isFinite(x))return '—';return x.toFixed(c).replace('.',',').replace('-','−')}
function vs(m){var s=m.sigma>0?m.sigma:(m.sigma_mais>0?(m.sigma_mais+m.sigma_menos)/2:null);var c=casas(s);if(m.sigma>0)return fmt(m.valor,c)+' ± '+fmt(m.sigma,c);if(m.sigma_mais>0)return fmt(m.valor,c)+' +'+fmt(m.sigma_mais,c)+'/−'+fmt(m.sigma_menos,c);return fmt(m.valor,4)}
function pad(n){return 'C'+String(n).padStart(2,'0')}
/* ── URB1 no navegador: só o espelho do quadro da pergunta (CRC16-CCITT 0x1021, init 0xFFFF) ── */
function crc16(bytes){var crc=0xFFFF;for(var i=0;i<bytes.length;i++){crc^=bytes[i]<<8;for(var b=0;b<8;b++)crc=(crc&0x8000)?((crc<<1)^0x1021)&0xFFFF:(crc<<1)&0xFFFF}return crc}
function hex(bytes){var o='';for(var i=0;i<bytes.length;i++)o+=bytes[i].toString(16).padStart(2,'0');return o.toUpperCase()}
function espelho(){var t=$('input').value;$('charc').textContent=t.length+' / 2000';var corpo=new TextEncoder().encode(JSON.stringify({de:'painel',para:'*',pergunta:t}));
  var cab=[0x55,0x52,0x42,0x31,1,1,(corpo.length>>8)&255,corpo.length&255];var sem=new Uint8Array(cab.length+corpo.length);sem.set(cab);sem.set(corpo,8);var crc=crc16(sem);
  $('mirror').innerHTML='55524231 01 01 '+hex(cab.slice(6))+' '+esc(hex(corpo).slice(0,96))+(corpo.length>48?'…':'')+' <em>'+crc.toString(16).toUpperCase().padStart(4,'0')+'</em>'}
/* ── token lembrado neste aparelho ── */
try{var t0=localStorage.getItem('parallax-token');if(t0&&$('tok').type==='password')$('tok').value=t0}catch(x){}
function guardarToken(){try{localStorage.setItem('parallax-token',$('tok').value)}catch(x){}}
$('tok').addEventListener('input',guardarToken);$('tok').addEventListener('change',guardarToken);
var cabAuth=function(){return {'authorization':'Bearer '+$('tok').value}};
/* ── boot ── */
function chips(){var h=$('cels');h.innerHTML='';est.lista.forEach(function(c){var x=document.createElement('span');x.className='cel-chip'+(est.sel[c.id]!==false?' on':'');x.innerHTML='<b>'+pad(c.n)+'</b>'+esc(c.nome);x.title=c.sondas+' · '+c.status;x.onclick=function(){est.sel[c.id]=est.sel[c.id]===false;x.classList.toggle('on')};h.appendChild(x)})}
function selecionadas(){return est.lista.filter(function(c){return est.sel[c.id]!==false}).map(function(c){return c.id})}
function iniciar(celulas,rodadas){$('launch').disabled=true;$('health').className='';$('health').textContent='iniciando…';
  fetch('/api/ciclo',{method:'POST',headers:Object.assign({'content-type':'application/json'},cabAuth()),body:JSON.stringify({objetivo:$('input').value,celulas:celulas,rodadas:rodadas,parecer:$('par').checked})})
  .then(function(r){return r.json().then(function(j){return [r.status,j]})}).then(function(a){$('launch').disabled=false;if(a[0]>=300){$('health').className='err';$('health').textContent=a[1].erro||('HTTP '+a[0]);return}$('health').textContent='ciclo '+a[1].id+' iniciado: '+a[1].celulas.length+' células, '+a[1].rodadas+' rodada(s), modo '+a[1].modo;mostrarDeck()})
  .catch(function(e){$('launch').disabled=false;$('health').className='err';$('health').textContent='falha ao iniciar: '+e})}
$('launch').onclick=function(){iniciar(selecionadas(),Number($('rod').value))};
$('bCurto').onclick=function(){iniciar(['desi','des','hubble','planck'],1)};
$('bDeck').onclick=function(){mostrarDeck()};
$('bTes').onclick=function(){$('testes').innerHTML='<p class="boot-note">testando…</p>';fetch('/api/testar',{headers:cabAuth()}).then(function(r){return r.json()}).then(function(j){
  if(j.erro){$('testes').innerHTML='<p class="boot-note" style="color:var(--red)">'+esc(j.erro)+'</p>';return}
  $('testes').innerHTML='<table class="t"><tr><th>conexão</th><th>resultado</th><th>ms</th><th>nota</th></tr>'+j.resultados.map(function(r){return '<tr><td>'+esc(r.nome)+'</td><td class="'+(r.ok?'ok':'falha')+'">'+(r.ok?'ok':'falhou')+'</td><td class="num">'+(r.ms==null?'—':r.ms)+'</td><td>'+esc(r.nota||'')+'</td></tr>'}).join('')+'</table>'}).catch(function(e){$('testes').innerHTML='<p class="boot-note" style="color:var(--red)">'+esc(e)+'</p>'})};
function listarDossies(){fetch('/api/dossies').then(function(r){return r.json()}).then(function(j){var h=$('dossies');if(!j.dossies||!j.dossies.length){h.innerHTML='<div class="field-label"><span class="eyebrow">Dossiês</span><span>nenhum salvo neste disco</span></div>';return}
  h.innerHTML='<div class="field-label"><span class="eyebrow">Dossiês</span><span>'+j.dossies.length+' · clique para reler sem gastar token</span></div>'+j.dossies.slice(0,20).map(function(d){return '<div class="dossie" data-id="'+esc(d.id)+'"><b>'+esc(d.id)+'</b><span>'+esc(d.quando.replace('T',' ').slice(0,16))+'</span><span>'+Math.round(d.bytes/1024)+' KB</span></div>'}).join('');
  Array.prototype.forEach.call(h.querySelectorAll('.dossie'),function(el){el.onclick=function(){carregarDossie(el.getAttribute('data-id'))}})}).catch(function(){})}
/* ── estado das células ── */
function novo(c){return {id:c.id,n:c.n,nome:c.nome,status:c.status,banda:c.banda,papeis:{},peso:1,suprimida:false,violacoes:0,digest:[],hex:'',motivos:[],estado:'idle'}}
function resetar(ids){est.celulas={};est.ordem=[];est.urb=0;est.trafego={};est.tabela=null;est.registros=[];est.parecer='';est.chamadas=0;est.custo=null;
  ids.forEach(function(id){var c=est.porId[id];if(c){est.celulas[id]=novo(c);est.ordem.push(id)}});$('reduce').innerHTML='<p class="vazio">A síntese aparece aqui quando a redução e o parecer concluírem.</p>';$('busline').textContent='barramento ocioso';$('busline').className='';desenharGrade()}
function cardHTML(s){var ativo=Object.keys(s.papeis).some(function(p){return s.papeis[p].estado==='ativo'});var erro=Object.keys(s.papeis).some(function(p){return s.papeis[p].estado==='erro'});var fim=s.papeis.relator&&s.papeis.relator.estado&&s.papeis.relator.estado!=='ativo'&&s.papeis.relator.estado!=='aguardando';
  var cls='cl'+(ativo?' busy':fim?' done':'')+(erro?' fail':'')+(s.suprimida?' sup':'');
  var pp=PAP.map(function(p){var e=(s.papeis[p[0]]||{}).estado||'aguardando';return '<div class="pp '+e+'" title="'+NOME_PAP[p[0]]+' · '+e+'">'+p[1]+'</div>'}).join('');
  var txt=s.digest&&s.digest.length?esc(s.digest.join('\\n')):'';
  return '<div class="'+cls+'" id="cel-'+s.id+'" data-id="'+s.id+'"><div class="cl-head"><span class="dot"></span><span class="cl-id">'+pad(s.n)+'</span><span class="cl-title">'+esc(s.nome)+'</span><span class="cl-stat" title="'+esc(s.status)+' · peso '+fmt(s.peso,2)+(s.suprimida?' · suprimida':'')+(s.violacoes?' · '+s.violacoes+' violação(ões)':'')+'">'+(s.suprimida?'SUPRIMIDA · ':'')+(s.violacoes?s.violacoes+' viol. · ':'')+'peso '+fmt(s.peso,2)+'</span></div>'+
    '<div class="cl-body"><div class="papeis">'+pp+'</div><div class="pane"><div class="pane-h"><span class="n">DIGEST</span><span class="t">'+esc(s.banda||'')+'</span><span class="m">'+(ativo?'em execução':fim?'concluído':'aguardando')+'</span></div><div class="pane-txt'+(txt?'':' vazio')+'">'+(txt||'sem digest ainda')+'</div><div class="pane-hex">'+esc(s.hex||'')+'</div></div></div><div class="peso"><i style="width:'+Math.round(Math.max(0,Math.min(1,s.peso))*100)+'%"></i></div></div>'}
function desenharGrade(){var g=$('grid');g.innerHTML=est.ordem.map(function(id){return cardHTML(est.celulas[id])}).join('');Array.prototype.forEach.call(g.querySelectorAll('.cl'),function(el){el.onclick=function(){abrir(el.getAttribute('data-id'))}});$('s-cel').textContent=est.ordem.length}
function atualizarCard(id){var s=est.celulas[id];if(!s)return;var velho=$('cel-'+id);if(!velho)return;var tmp=document.createElement('div');tmp.innerHTML=cardHTML(s);var novoEl=tmp.firstChild;novoEl.onclick=function(){abrir(id)};velho.replaceWith(novoEl)}
function fase(txt,cls){var p=$('phase');p.textContent=txt;p.className='phase'+(cls?' '+cls:'')}
function stats(){$('s-frames').textContent=est.urb;$('s-call').textContent=est.chamadas;var c=est.custo;$('s-usd').textContent=est.modo==='real'?(c?'≈US$ '+fmt(c.usd,2)+(est.teto?' / '+est.teto:''):'—'):'simulado';
  if(est.inicio)$('s-time').textContent=Math.round((Date.now()-est.inicio)/1000)+'s'}
function cronometro(ligar){if(est.timer){clearInterval(est.timer);est.timer=null}if(ligar)est.timer=setInterval(stats,1000)}
function mostrarDeck(){$('boot').classList.add('oculto');$('deck').classList.add('on');document.body.classList.add('deck-on')}
function mostrarBoot(){$('deck').classList.remove('on');$('boot').classList.remove('oculto');document.body.classList.remove('deck-on');listarDossies()}
$('btn-reset').onclick=function(){fecharFocus();$('final').classList.remove('on');mostrarBoot()};
$('btn-stop').onclick=function(){fetch('/api/parar',{method:'POST',headers:cabAuth()}).then(function(r){return r.json()}).then(function(j){bus(j.parado?'interrompendo o ciclo…':'nada a parar','a')}).catch(function(){})};
$('btn-final').onclick=function(){abrirFinal()};
$('fin-close').onclick=function(){$('final').classList.remove('on')};
function bus(txt,cls){var b=$('busline');b.textContent=txt;b.className=cls||''}
/* ── eventos ── */
function evento(ev){
  if(ev.t==='ciclo'){if(ev.celulas){est.fonte='vivo';est.ciclo=ev.id;est.inicio=ev.ts||Date.now();resetar(ev.celulas);cronometro(true);$('btn-stop').disabled=false;mostrarDeck()}
    if(ev.estado==='executando'){var f=ev.fase||'local';fase(/^gossip/.test(f)?'02 GOSSIP '+f.replace('gossip ',''):(FASE[f]||f.toUpperCase()))}}
  else if(ev.t==='papel'){if(ev.celula==='rede'){if(ev.estado==='ativo')fase('05 PARECER');return}var s=est.celulas[ev.celula];if(!s)return;s.papeis[ev.papel]={estado:ev.estado};if(ev.violacoes&&ev.violacoes.length)s.violacoes+=ev.violacoes.length;if(ev.digest&&ev.digest.length&&(ev.papel==='relator'||ev.papel==='embaixador'||!s.digest.length))s.digest=ev.digest;atualizarCard(ev.celula);
    if(ev.estado!=='ativo')bus(pad(s.n)+' '+ev.celula+' · '+NOME_PAP[ev.papel]+(ev.rodada?' r'+ev.rodada:'')+(ev.final?' final':'')+' → '+ev.estado+(ev.erro?' · '+ev.erro:'')+(ev.violacoes&&ev.violacoes.length?' · '+ev.violacoes.length+' violação(ões)':''),ev.estado==='erro'||ev.estado==='violacao'?'e':'');
    if(est.foco===ev.celula&&ev.estado!=='ativo')abrir(ev.celula,true)}
  else if(ev.t==='peso'){var s2=est.celulas[ev.celula];if(!s2)return;s2.peso=ev.peso;s2.suprimida=ev.suprimida;s2.motivos=ev.motivos||[];atualizarCard(ev.celula);if(ev.suprimida)bus('SUPRIMIDA '+ev.celula+' · peso '+fmt(ev.peso,2)+' · '+ev.motivos.join(', '),'a')}
  else if(ev.t==='urb1'){est.urb++;var k=ev.tipo+' '+ev.de+'→'+ev.para;est.trafego[k]=(est.trafego[k]||0)+1;bus(ev.tipo+' '+ev.de+'→'+ev.para+' · '+ev.bytes+' B · '+ev.hex);var d=est.celulas[ev.de];if(d){d.hex=ev.hex;atualizarCard(ev.de)}stats()}
  else if(ev.t==='gossip'){fase('02 GOSSIP '+ev.rodada+' · Δ '+fmt(ev.delta,3));bus('gossip rodada '+ev.rodada+' · Δ = '+fmt(ev.delta,3)+' · '+ev.ativas+' células ativas')}
  else if(ev.t==='ferramenta'){if(ev.erro)bus(ev.celula+' · '+ev.papel+' · '+ev.nome+' · '+ev.erro,'a')}
  else if(ev.t==='custo'){est.custo=ev;est.chamadas=ev.chamadas||0;stats()}
  else if(ev.t==='rede'){if(ev.tabela)est.tabela=ev.tabela;if(ev.registros)est.registros=ev.registros}
  else if(ev.t==='log'){bus(ev.msg,ev.nivel==='erro'?'e':(ev.nivel==='alerta'?'a':''))}
  else if(ev.t==='fim'){cronometro(false);stats();$('btn-stop').disabled=true;fase(ev.estado==='concluido'?(ev.aviso?'CONCLUÍDO · TETO':'CONCLUÍDO'):ev.estado.toUpperCase(),ev.estado==='concluido'?(ev.aviso?'warn':''):'err');bus('fim '+ev.id+' · '+ev.estado+' · '+ev.duracao_s+' s'+(ev.erro?' · '+ev.erro:'')+(ev.aviso?' · '+ev.aviso:''),ev.estado==='concluido'?'':'e');
    fetch('/api/estado').then(function(r){return r.json()}).then(function(e){var cc=e.ciclo;if(cc&&cc.id===est.ciclo){est.parecer=(cc.rede&&cc.rede.texto)||'';if(cc.tabela)est.tabela=cc.tabela;Object.keys(cc.celulas||{}).forEach(function(id){var s=est.celulas[id];if(s){s.peso=cc.celulas[id].peso;s.suprimida=cc.celulas[id].suprimida;s.digest=cc.celulas[id].digest||s.digest;atualizarCard(id)}});if(ev.estado==='concluido')abrirFinal()}}).catch(function(){})}}
/* ── visão focada ── */
function abrir(id,manter){est.foco=id;var s=est.celulas[id];if(!s)return;var c=est.porId[id]||{};$('f-title').textContent=pad(s.n)+' · '+s.nome;$('f-stat').textContent='peso '+fmt(s.peso,2)+(s.suprimida?' · SUPRIMIDA':'');$('f-stat').className='phase'+(s.suprimida?' err':'');$('f-sub').textContent=(c.telescopio||'')+' · '+(c.sondas||'')+(s.motivos.length?' · '+s.motivos.join(' · '):'');
  if(!manter)$('f-body').innerHTML='<div class="f-pane"><div class="f-meta">carregando…</div></div>';$('focus').classList.add('on');
  var p=est.fonte==='dossie'?Promise.resolve(s.detalhe||{}):fetch('/api/celula/'+encodeURIComponent(id)).then(function(r){return r.json()});
  p.then(function(d){if(est.foco!==id)return;var ex=(d&&d.execucoes&&d.execucoes.length)?d.execucoes:[];var o='';
    if(!ex.length){o='<div class="f-pane"><h4 class="eyebrow">—</h4><div class="f-meta">'+(d&&d.papeis?'nenhum papel concluído ainda':'sem execução desta célula nesta sessão')+'</div></div>'}
    ex.forEach(function(x,i){var g=x.guarda||{violacoes:[]};o+='<div class="f-pane'+(ADVERSO[x.papel]?' i':'')+'"><h4 class="eyebrow">'+String(i+1).padStart(2,'0')+' · '+NOME_PAP[x.papel]+(x.rodada?' · rodada '+x.rodada:'')+(x.final?' · final':'')+'</h4><div class="f-meta">'+esc(x.estado)+' · '+Math.round((x.ms||0)/1000)+' s · guarda: '+(g.conferidos||0)+' conferido(s), '+g.violacoes.length+' violação(ões)</div><div class="f-txt">'+esc(x.texto||'')+'</div>'+(g.violacoes.length?'<div class="f-viol">'+g.violacoes.map(function(v){return esc(v.numero)+' — “'+esc(v.trecho)+'”'+(v.motivo?' ('+esc(v.motivo)+')':'')}).join('<br>')+'</div>':'')+'</div>'});
    if(d&&d.papeis)Object.keys(d.papeis).forEach(function(pn){var v=d.papeis[pn];if(v.estado==='erro')o+='<div class="f-pane"><h4 class="eyebrow">'+NOME_PAP[pn]+'</h4><div class="f-erro">'+esc(v.erro||'erro')+'</div></div>';else if(v.estado==='ativo')o+='<div class="f-pane"><h4 class="eyebrow">'+NOME_PAP[pn]+'</h4><div class="f-meta">em execução…</div></div>'});
    $('f-body').innerHTML=o}).catch(function(e){$('f-body').innerHTML='<div class="f-pane"><div class="f-erro">'+esc(e)+'</div></div>'})}
function fecharFocus(){$('focus').classList.remove('on');est.foco=null}
function navegar(d){if(!est.foco)return;var i=est.ordem.indexOf(est.foco);if(i<0)return;abrir(est.ordem[(i+d+est.ordem.length)%est.ordem.length])}
$('f-close').onclick=fecharFocus;$('f-prev').onclick=function(){navegar(-1)};$('f-next').onclick=function(){navegar(1)};
/* ── síntese: mapa de tráfego + parecer + rede + Polo V ── */
function mapa(){var n=est.ordem.length,R=118,cx=150,cy=150,pos={};est.ordem.forEach(function(id,i){var a=-Math.PI/2+2*Math.PI*i/n;pos[id]=[cx+R*Math.cos(a),cy+R*Math.sin(a)]});
  var o='',max=1;Object.keys(est.trafego).forEach(function(k){if(est.trafego[k]>max)max=est.trafego[k]});
  Object.keys(est.trafego).forEach(function(k){var m=/^(\\w+) (\\S+)→(\\S+)$/.exec(k);if(!m)return;var a=pos[m[2]],b=pos[m[3]];var w=.6+2.4*est.trafego[k]/max;
    if(m[1]==='GOSSIP'&&a&&b)o+='<line x1="'+a[0]+'" y1="'+a[1]+'" x2="'+b[0]+'" y2="'+b[1]+'" stroke="#35e0c0" stroke-opacity=".55" stroke-width="'+w.toFixed(1)+'"/>';
    else if(m[1]==='MEDIDA'&&a)o+='<line x1="'+a[0]+'" y1="'+a[1]+'" x2="'+cx+'" y2="'+cy+'" stroke="#ffb44a" stroke-opacity=".5" stroke-width="'+w.toFixed(1)+'"/>'});
  o+='<circle cx="'+cx+'" cy="'+cy+'" r="14" fill="#070b11" stroke="#ffb44a" stroke-opacity=".6"/><text x="'+cx+'" y="'+(cy+3)+'" text-anchor="middle" font-size="7" fill="#ffb44a" font-family="JetBrains Mono,monospace">REDE</text>';
  est.ordem.forEach(function(id){var s=est.celulas[id],p=pos[id];var col=s.suprimida?'#ff5f56':'#35e0c0';o+='<circle cx="'+p[0]+'" cy="'+p[1]+'" r="9" fill="#070b11" stroke="'+col+'"/><text x="'+p[0]+'" y="'+(p[1]+2.5)+'" text-anchor="middle" font-size="6.5" fill="'+col+'" font-family="JetBrains Mono,monospace">'+String(s.n).padStart(2,'0')+'</text>';
    var lx=cx+(R+22)*(p[0]-cx)/R,ly=cy+(R+22)*(p[1]-cy)/R;o+='<text x="'+lx+'" y="'+(ly+2.5)+'" text-anchor="middle" font-size="5.5" fill="#4c5f72" font-family="JetBrains Mono,monospace">'+esc(id)+'</text'+'>'});
  $('map').innerHTML=o;var sup=est.ordem.filter(function(id){return est.celulas[id].suprimida});
  $('kv').innerHTML='<b>'+est.ordem.length+'</b> células · <b>'+sup.length+'</b> suprimida(s)'+(sup.length?' ('+sup.join(', ')+')':'')+'<br><b>'+est.urb+'</b> quadros URB1 · <b>'+est.chamadas+'</b> chamadas'+(est.custo&&est.modo==='real'?' · ≈US$ <b>'+fmt(est.custo.usd,2)+'</b> (estimado pelos campos usage)':'')}
function redeHTML(){var t=est.tabela;if(!t||!Object.keys(t).length)return '<p class="vazio">nenhuma medida publicada</p>';var o='';Object.keys(t).forEach(function(g){var b=t[g];o+='<h3>'+esc(g)+'</h3><div class="tab-wrap"><table class="t"><tr><th>célula</th><th>valor</th><th>modelo</th><th>id</th></tr>'+b.medidas.map(function(m){return '<tr><td>'+esc(m.celula)+'</td><td class="num">'+vs(m)+'</td><td>'+esc(m.modelo||'')+'</td><td>'+esc(m.tag)+' · '+esc(m.id)+'</td></tr>'}).join('')+'</table></div>';
    var ps=b.pares.filter(function(p){return p.sigma>=1});if(ps.length)o+='<div class="avisos">'+ps.map(function(p){var cl=!p.independentes?'dep':(p.sigma>=est.limiarTensao?'tens':'');return '<span class="'+cl+'">'+esc(p.celulas.join(' × '))+': '+fmt(p.sigma,1)+'σ'+(p.independentes?'':' (não indep.)')+(p.mesmoModelo?'':' (modelos ≠)')+'</span>'}).join(' · ')+'</div>'});return o}
function polovHTML(){var regs=est.registros||[];if(!regs.length)return '<p class="vazio">sem registros da redução</p>';return '<div class="tab-wrap"><table class="t"><tr><th>registro</th><th>valor</th><th>id</th></tr>'+regs.map(function(r){return '<tr><td>'+esc(r.rotulo||r.grandeza||'')+(r.avisos?'<div class="avisos">'+esc(r.avisos.join('; '))+'</div>':'')+'</td><td class="num">'+(r.valor!=null?vs(r)+(r.unidade?' '+esc(r.unidade):''):'—')+'</td><td>'+esc(r.id)+'</td></tr>'}).join('')+'</table></div>'}
function abrirFinal(){mapa();$('reduce').innerHTML='<h2>01 · Parecer da rede</h2>'+(est.parecer?'<pre>'+esc(est.parecer)+'</pre>':'<p class="vazio">'+(est.ciclo?'sem parecer (ciclo sem parecer, interrompido ou com teto atingido)':'nenhum ciclo')+'</p>')+'<h2>02 · Rede · medidas publicadas</h2>'+redeHTML()+'<h2>03 · Polo V · redução determinística</h2>'+polovHTML();$('final').classList.add('on')}
/* ── dossiê salvo: relê um ciclo sem gastar token ── */
function carregarDossie(id){$('health').textContent='lendo '+id+'…';fetch('/api/dossies/'+encodeURIComponent(id)).then(function(r){return r.json()}).then(function(d){
  est.fonte='dossie';est.ciclo=d.id;est.inicio=0;cronometro(false);var ids=Object.keys(d.celulasCompletas||d.celulas||{});resetar(ids);
  ids.forEach(function(cid){var s=est.celulas[cid],x=(d.celulasCompletas||{})[cid]||d.celulas[cid];if(!s||!x)return;s.peso=x.peso;s.suprimida=x.suprimida;s.violacoes=x.violacoes||0;s.digest=x.digest||[];s.motivos=x.motivosPeso||[];Object.keys(x.papeis||{}).forEach(function(p){s.papeis[p]={estado:x.papeis[p].estado}});s.detalhe={execucoes:x.execucoes||[],papeis:x.papeis||{}}});
  est.urb=(d.urb1&&d.urb1.quadros)||0;est.trafego=(d.urb1&&d.urb1.trafego)||{};est.chamadas=(d.custo&&d.custo.chamadas)||0;est.custo=d.custo||null;est.tabela=d.tabela||null;est.parecer=(d.rede&&d.rede.texto)||'';est.registros=(d.registros||[]).filter(function(r){return r.celula==='rede'});
  desenharGrade();stats();$('s-time').textContent=d.fim&&d.inicio?Math.round((d.fim-d.inicio)/1000)+'s':'—';fase('DOSSIÊ · '+(d.estado||'').toUpperCase(),d.estado==='concluido'?'':'warn');bus('dossiê '+d.id+' relido do disco · '+(d.objetivo||''));$('btn-stop').disabled=true;mostrarDeck()}).catch(function(e){$('health').className='err';$('health').textContent='não consegui ler o dossiê: '+e})}
/* ── atalhos ── */
document.addEventListener('keydown',function(e){if(e.key==='Escape'){if($('focus').classList.contains('on'))fecharFocus();else $('final').classList.remove('on')}
  if($('boot').classList.contains('oculto')){if(e.key==='ArrowLeft'&&est.foco)navegar(-1);if(e.key==='ArrowRight'&&est.foco)navegar(1);if((e.key==='s'||e.key==='S')&&!$('focus').classList.contains('on')&&document.activeElement.tagName!=='TEXTAREA'&&document.activeElement.tagName!=='INPUT')abrirFinal()}
  else if(e.key==='Enter'&&(e.ctrlKey||e.metaKey)){$('launch').click()}});
$('input').addEventListener('input',espelho);
/* ── ligação ── */
function aplicarEstado(e){est.modo=e.modo;est.teto=e.teto;est.lista=e.celulas;est.porId={};e.celulas.forEach(function(c){est.porId[c.id]=c});est.limiarPeso=e.limiarPeso||.5;est.limiarTensao=e.limiarTensao||3;chips();
  $('marca-sub').textContent=e.celulas.length+' células · 8 × '+(e.modelo||'Claude Sonnet')+' · v'+e.versao;
  $('health').className='';$('health').textContent='modo '+e.modo+(e.modo==='simulado'?' (sem ANTHROPIC_API_KEY: nenhuma chamada à API)':' · teto US$ '+e.teto+' por ciclo (estimativa pelos campos usage)')+(e.ciclo?' · último ciclo '+e.ciclo.id+' · '+e.ciclo.estado:'');
  var c=e.ciclo;if(c&&c.estado==='executando'){est.fonte='vivo';est.ciclo=c.id;est.inicio=c.inicio||Date.now();resetar(Object.keys(c.celulas));Object.keys(c.celulas).forEach(function(id){var s=est.celulas[id],x=c.celulas[id];s.peso=x.peso;s.suprimida=x.suprimida;s.violacoes=x.violacoes;s.digest=x.digest||[];Object.keys(x.papeis).forEach(function(p){s.papeis[p]={estado:x.papeis[p].estado}})});est.urb=(c.urb1&&c.urb1.quadros)||0;est.trafego=(c.urb1&&c.urb1.trafego)||{};est.custo=c.custo;est.chamadas=(c.custo&&c.custo.chamadas)||0;est.tabela=c.tabela||null;desenharGrade();stats();cronometro(true);fase(/^gossip/.test(c.fase)?'02 GOSSIP '+c.fase.replace('gossip ',''):(FASE[c.fase]||String(c.fase).toUpperCase()));$('btn-stop').disabled=false;mostrarDeck()}
  stats()}
function ligar(){fetch('/api/estado').then(function(r){return r.json()}).then(function(e){aplicarEstado(e);listarDossies();espelho();
  var es=new EventSource('/api/eventos?desde='+(e.seq||0));es.onerror=function(){bus('SSE reconectando…','a')};es.onopen=function(){if($('busline').textContent==='SSE reconectando…')bus('barramento ligado')};
  es.onmessage=function(m){try{evento(JSON.parse(m.data))}catch(x){}}}).catch(function(){$('health').className='err';$('health').textContent='servidor indisponível, tentando de novo…';setTimeout(ligar,3000)})}
ligar();
})();
</script></body></html>`;
  }
  return { html };
})();

/* ── 18 · servidor HTTP (painel, SSE, API) ─────────────────────────────────
 * Leitura é pública (painel, estado, eventos, registro, dossiês). O que gasta
 * API ou sai para a rede (ciclo, parar, testar) exige ACCESS_TOKEN; em
 * produção (Render define RENDER) essas rotas ficam fechadas sem o token.
 */
const SERV = (() => {
  const clientes = new Set();
  // token exigido em produção, quando definido, ou sempre que houver chave real (nunca deixar a API aberta)
  const exigeToken = () => CFG.PRODUCAO || !!CFG.ACCESS_TOKEN || CFG.MODO === 'real';
  const MAX_SSE = 50, REPLAY_SSE = 500;
  const RE_DOSSIE = /^CIC-\d{14}-[0-9a-f]{4}$/;

  function cabecalhos(res, extra = {}) {
    res.setHeader('x-content-type-options', 'nosniff');
    res.setHeader('referrer-policy', 'no-referrer');
    res.setHeader('x-frame-options', 'DENY');
    res.setHeader('cross-origin-opener-policy', 'same-origin');
    res.setHeader('permissions-policy', 'camera=(), microphone=(), geolocation=()');
    for (const [k, v] of Object.entries(extra)) res.setHeader(k, v);
  }
  function json(res, status, obj) {
    const corpo = JSON.stringify(obj);
    cabecalhos(res, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.writeHead(status);
    res.end(corpo);
  }
  function autorizar(req) {
    if (!exigeToken()) return null;
    if (!CFG.ACCESS_TOKEN) return { status: 503, erro: 'ACCESS_TOKEN não configurado. Em produção, ciclo, parar e testar ficam fechados até definir ACCESS_TOKEN nas variáveis do serviço.' };
    const h = String(req.headers.authorization || '');
    const tok = h.startsWith('Bearer ') ? h.slice(7).trim() : String(req.headers['x-access-token'] || '');
    if (!tok || !U.iguaisSeguro(tok, CFG.ACCESS_TOKEN)) return { status: 401, erro: 'token inválido ou ausente' };
    return null;
  }
  function lerCorpo(req, limite = 65536) {
    return new Promise((resolve, reject) => {
      let tam = 0, estourou = false;
      const partes = [];
      req.on('data', (c) => { tam += c.length; if (tam > limite) estourou = true; else partes.push(c); });
      req.on('end', () => (estourou ? reject(Object.assign(new Error('corpo excede ' + limite + ' bytes'), { status: 413 })) : resolve(Buffer.concat(partes).toString('utf8'))));
      req.on('error', reject);
    });
  }

  function sse(req, res, url) {
    if (clientes.size >= MAX_SSE) return json(res, 503, { erro: 'limite de ' + MAX_SSE + ' conexões de eventos atingido' });
    cabecalhos(res, { 'content-type': 'text/event-stream; charset=utf-8', 'cache-control': 'no-store', connection: 'keep-alive', 'x-accel-buffering': 'no' });
    res.writeHead(200);
    res.write('retry: 3000\n\n');
    const desde = Number(req.headers['last-event-id'] || url.searchParams.get('desde') || 0);
    const enviar = (ev) => { try { res.write('id: ' + ev.seq + '\ndata: ' + JSON.stringify(ev) + '\n\n'); } catch (e) { /* cliente caiu */ } };
    if (desde > 0) { const h = ORQ.historico; for (let i = Math.max(0, h.length - REPLAY_SSE); i < h.length; i++) if (h[i].seq > desde) enviar(h[i]); }
    const batida = setInterval(() => { try { res.write(': batida\n\n'); } catch (e) { /* */ } }, 15000);
    const cli = { res, batida, ouvinte: enviar };
    ORQ.ouvintes.add(enviar);
    clientes.add(cli);
    req.on('close', () => { clearInterval(batida); ORQ.ouvintes.delete(enviar); clientes.delete(cli); });
  }

  /* Diagnóstico ao vivo: API, serviços TAP e produtos de dados. */
  async function testar() {
    const resultados = [];
    const medir = async (nome, f) => {
      const t0 = Date.now();
      try { const r = await f(); resultados.push({ nome, ok: r.ok !== false, ms: Date.now() - t0, nota: r.nota || '' }); }
      catch (e) { resultados.push({ nome, ok: false, ms: Date.now() - t0, nota: U.causa(e).slice(0, 240) }); }
    };
    const ua = { 'user-agent': 'parallax-celulas/' + VERSAO };
    const tarefas = [];
    if (CFG.MODO === 'real') {
      const cab = { 'x-api-key': CFG.ANTHROPIC_API_KEY, 'anthropic-version': CFG.ANTHROPIC_VERSION };
      tarefas.push(medir('Anthropic · GET /v1/models/' + CFG.MODELO, async () => {
        const r = await fetch(new URL('/v1/models/' + encodeURIComponent(CFG.MODELO), CFG.ANTHROPIC_URL), { headers: cab, signal: AbortSignal.timeout(30000) });
        const t = await r.text();
        if (!r.ok) return { ok: false, nota: IA.traduzir(r.status, t).msg };
        let j = {}; try { j = JSON.parse(t); } catch (e) { /* */ }
        return { ok: true, nota: 'modelo existe' + (j.display_name ? ': ' + j.display_name : '') };
      }));
      tarefas.push(medir('Anthropic · POST /v1/messages (mesmo formato do ciclo, esforço low)', async () => {
        const corpo = {
          model: CFG.MODELO, max_tokens: 1024, system: 'Teste de conexão do PARALLAX · Células. Não use ferramentas.',
          tools: FERR.esquemas(['inventario']), tool_choice: { type: 'auto' }, output_config: { effort: 'low' },
          messages: [{ role: 'user', content: 'Responda apenas: ok' }],
        };
        const r = await fetch(CFG.ANTHROPIC_URL, { method: 'POST', headers: { ...cab, 'content-type': 'application/json' }, body: JSON.stringify(corpo), signal: AbortSignal.timeout(120000) });
        const t = await r.text();
        if (!r.ok) return { ok: false, nota: IA.traduzir(r.status, t).msg };
        const j = JSON.parse(t);
        const u = j.usage || {};
        return { ok: true, nota: 'stop_reason ' + j.stop_reason + ' · blocos ' + (j.content || []).map((b) => b.type).join('+') + ' · tokens ' + (u.input_tokens || 0) + ' entrada / ' + (u.output_tokens || 0) + ' saída' };
      }));
    } else {
      resultados.push({ nome: 'Anthropic', ok: false, ms: null, nota: 'modo simulado: ANTHROPIC_API_KEY ausente' });
    }
    for (const s of Object.keys(TAP.SERVICOS)) {
      tarefas.push(medir('TAP ' + s + ' · ' + TAP.SERVICOS[s].url, async () => { const r = await TAP.testar(s); return { ok: r.ok, nota: 'HTTP ' + r.status + (r.ok ? '' : ' (capabilities não reconhecido)') }; }));
    }
    for (const [id, p] of Object.entries(CELULAS.PRODUTOS)) {
      tarefas.push(medir('produto ' + id, async () => {
        const r = await fetch(p.url, { method: 'HEAD', headers: ua, signal: AbortSignal.timeout(30000) });
        const tam = r.headers.get('content-length');
        return { ok: r.ok, nota: 'HTTP ' + r.status + (tam ? ' · ' + tam + ' bytes' : '') };
      }));
    }
    await Promise.all(tarefas);
    const ordem = (n) => (n.startsWith('Anthropic') ? 0 : n.startsWith('TAP') ? 1 : 2);
    return resultados.sort((a, b) => ordem(a.nome) - ordem(b.nome) || a.nome.localeCompare(b.nome));
  }

  function listarDossies() {
    const dir = path.join(CFG.DATA_DIR, 'dossies');
    try {
      return fs.readdirSync(dir).filter((f) => f.endsWith('.json')).map((f) => {
        const st = fs.statSync(path.join(dir, f));
        return { id: f.replace(/\.json$/, ''), bytes: st.size, quando: st.mtime.toISOString() };
      }).sort((a, b) => b.quando.localeCompare(a.quando));
    } catch (e) { return []; }
  }

  async function rotear(req, res) {
    const url = new URL(req.url, 'http://localhost');
    const rota = url.pathname;
    const m = req.method;
    if (m === 'GET' && rota === '/') {
      const nonce = crypto.randomBytes(16).toString('base64');
      cabecalhos(res, {
        'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store',
        'content-security-policy': "default-src 'self'; script-src 'nonce-" + nonce + "'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; img-src 'self' data:; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
      });
      res.writeHead(200);
      return res.end(PAINEL.html(nonce, exigeToken()));
    }
    if (m === 'GET' && rota === '/favicon.ico') { res.writeHead(204); return res.end(); }
    if ((m === 'GET' || m === 'HEAD') && rota === '/fundo.webp') {
      if (req.headers['if-none-match'] === FUNDO.etag) { res.writeHead(304); return res.end(); }
      cabecalhos(res, { 'content-type': FUNDO.tipo, 'content-length': String(FUNDO.bytes), 'cache-control': 'public, max-age=604800, immutable', etag: FUNDO.etag });
      res.writeHead(200);
      return res.end(m === 'HEAD' ? undefined : FUNDO.buf);
    }
    if ((m === 'GET' || m === 'HEAD') && rota === '/saude') {
      return json(res, 200, { ok: true, versao: VERSAO, modo: CFG.MODO, modelo: CFG.MODELO, uptime_s: Math.round(process.uptime()), ciclo: ORQ.atual() ? { id: ORQ.atual().id, estado: ORQ.atual().estado } : null });
    }
    if (m === 'GET' && rota === '/api/estado') return json(res, 200, ORQ.estado());
    if (m === 'GET' && rota === '/api/eventos') return sse(req, res, url);
    if (m === 'GET' && rota === '/api/ledger') {
      const f = {};
      if (url.searchParams.get('celula')) f.celula = url.searchParams.get('celula');
      if (url.searchParams.get('grandeza')) f.grandeza = url.searchParams.get('grandeza');
      if (url.searchParams.get('publicado')) f.publicado = url.searchParams.get('publicado') === '1';
      return json(res, 200, { registros: LEDGER.listar(f).map((e) => ({ ...e, numeros_guarda: undefined })) });
    }
    if (m === 'GET' && rota === '/api/dossies') return json(res, 200, { dossies: listarDossies() });
    if (m === 'GET' && rota.startsWith('/api/dossies/')) {
      const id = decodeURIComponent(rota.slice('/api/dossies/'.length));
      if (!RE_DOSSIE.test(id)) return json(res, 400, { erro: 'id de dossiê inválido' });
      const arq = path.join(CFG.DATA_DIR, 'dossies', id + '.json');
      if (!fs.existsSync(arq)) return json(res, 404, { erro: 'dossiê não encontrado (o disco do Render é efêmero sem disco persistente)' });
      cabecalhos(res, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'content-disposition': 'inline; filename="' + id + '.json"' });
      res.writeHead(200);
      const fluxo = fs.createReadStream(arq);
      fluxo.on('error', (e) => { console.error('[parallax] leitura do dossiê ' + id + ':', e.message); try { res.destroy(); } catch (x) { /* */ } });
      return fluxo.pipe(res);
    }
    if (m === 'GET' && rota.startsWith('/api/celula/')) {
      const id = decodeURIComponent(rota.slice('/api/celula/'.length));
      if (!Object.hasOwn(CELULAS.porId, id)) return json(res, 404, { erro: 'célula desconhecida' });
      return json(res, 200, ORQ.detalheCelula(id) || {});
    }
    if (m === 'POST' && rota === '/api/ciclo') {
      const neg = autorizar(req);
      if (neg) return json(res, neg.status, { erro: neg.erro });
      let op = {};
      try { const t = await lerCorpo(req); op = t ? JSON.parse(t) : {}; } catch (e) { return json(res, e.status || 400, { erro: e.status ? e.message : 'JSON inválido' }); }
      if (op.celulas && !Array.isArray(op.celulas)) return json(res, 400, { erro: 'celulas deve ser uma lista de ids' });
      const desconhecidas = (op.celulas || []).filter((id) => !Object.hasOwn(CELULAS.porId, id));
      if (desconhecidas.length) return json(res, 400, { erro: 'células desconhecidas: ' + desconhecidas.join(', ') });
      let info;
      try { info = await ORQ.executar(op); } catch (e) { return json(res, 409, { erro: e.message }); }
      return json(res, 202, info);
    }
    if (m === 'POST' && rota === '/api/parar') {
      const neg = autorizar(req);
      if (neg) return json(res, neg.status, { erro: neg.erro });
      return json(res, 200, { parado: ORQ.parar() });
    }
    if (m === 'GET' && rota === '/api/testar') {
      const neg = autorizar(req);
      if (neg) return json(res, neg.status, { erro: neg.erro });
      return json(res, 200, { modo: CFG.MODO, resultados: await testar() });
    }
    return json(res, 404, { erro: 'rota não encontrada' });
  }

  function iniciar() {
    LIT.carregar(); // registro visível antes do primeiro ciclo
    const servidor = http.createServer((req, res) => {
      rotear(req, res).catch((e) => {
        console.error('[parallax] erro na rota ' + req.url + ':', e);
        if (!res.headersSent) json(res, 500, { erro: 'erro interno: ' + e.message });
        else try { res.end(); } catch (x) { /* */ }
      });
    });
    servidor.requestTimeout = 0; // SSE e /api/testar podem durar
    servidor.keepAliveTimeout = 65000;
    servidor.headersTimeout = 66000; // deve ficar acima do keepAliveTimeout (conexões ociosas atrás do proxy)
    servidor.listen(CFG.PORT, CFG.HOST, () => {
      console.log('[parallax] PARALLAX · Células ' + VERSAO + ' em http://' + CFG.HOST + ':' + CFG.PORT);
      console.log('[parallax] modo ' + CFG.MODO + ' · modelo ' + CFG.MODELO + ' · esforço ' + CFG.ESFORCO + '/' + CFG.ESFORCO_CRITICO + ' · concorrência ' + CFG.CONCORRENCIA + ' · RPM ' + CFG.RPM + ' · teto US$ ' + CFG.TETO_USD + ' por ciclo');
      if (CFG.MODO === 'simulado') console.log('[parallax] sem ANTHROPIC_API_KEY: os papéis rodam no simulador determinístico (nenhuma chamada à API)');
      if (exigeToken() && !CFG.ACCESS_TOKEN) console.warn('[parallax] ATENÇÃO: sem ACCESS_TOKEN — ciclo, parar e testar ficam fechados (503) até definir a variável');
    });
    let encerrando = false;
    const encerrar = (sinal) => {
      if (encerrando) return;
      encerrando = true;
      console.log('[parallax] ' + sinal + ': encerrando');
      ORQ.parar();
      for (const c of clientes) { try { clearInterval(c.batida); c.res.end(); } catch (e) { /* */ } }
      servidor.close(() => process.exit(0));
      setTimeout(() => process.exit(0), 8000).unref();
    };
    process.on('SIGTERM', () => encerrar('SIGTERM'));
    process.on('SIGINT', () => encerrar('SIGINT'));
    process.on('unhandledRejection', (e) => console.error('[parallax] rejeição não tratada:', e));
    return servidor;
  }
  return { iniciar, rotear, testar, exigeToken };
})();

/* ── 19 · testes ────────────────────────────────────────────────────────────
 *   --teste        offline: protocolo, álgebra, cosmologia, leitores, guarda,
 *                  ledger, Polo V, laço de agente, painel, rotas HTTP e um
 *                  ciclo simulado de 4 células só com literatura
 *   --validar      baixa os dados públicos e as cadeias oficiais (DES-SN5YR e
 *                  Pantheon+), calcula os alvos na hora e compara os pipelines
 *   --simular      ciclo completo das 17 células no simulador, com uma célula
 *                  que inventa número (SIMULAR_ALUCINACAO, padrão euclid)
 *   --simular-api  ciclo num processo filho contra uma Messages API falsa que
 *                  confere cabeçalhos, pares tool_use/tool_result, devolução
 *                  dos blocos de thinking, e injeta 429 e 529
 */
const TESTES = (() => {
  const afirmar = (cond, msg) => { if (!cond) throw new Error(msg || 'condição falsa'); };
  const perto = (obtido, esperado, tol, rotulo) => { if (!(Math.abs(obtido - esperado) <= tol)) throw new Error((rotulo ? rotulo + ': ' : '') + 'esperado ' + esperado + ' ± ' + tol + ', obtido ' + obtido); };
  const lanca = (f, re, rotulo) => {
    try { f(); } catch (e) { if (re && !re.test(String(e.message))) throw new Error((rotulo || 'erro') + ': mensagem inesperada "' + e.message + '"'); return; }
    throw new Error((rotulo || 'deveria lançar erro') + ': não lançou');
  };
  async function rodar(lista) {
    let nOk = 0;
    const falhas = [];
    for (const [nome, f] of lista) {
      const t0 = Date.now();
      try {
        const nota = await f();
        nOk++;
        console.log('  ✓ ' + nome + (nota ? ' — ' + nota : '') + ' (' + (Date.now() - t0) + ' ms)');
      } catch (e) {
        falhas.push(nome);
        console.log('  ✗ ' + nome + ' — ' + (e && e.stack ? e.stack.split('\n').slice(0, 3).join(' | ') : e));
      }
    }
    return { nOk, falhas };
  }
  const tmpDir = (rotulo) => fs.mkdtempSync(path.join(require('node:os').tmpdir(), 'parallax-' + rotulo + '-'));

  /* ── construtores de arquivos sintéticos ─────────────────────────────── */
  function npySintetico(dtype, forma, valores) {
    const tam = { '<f8': 8, '<f4': 4, '<i8': 8, '<i4': 4 }[dtype];
    let cab = "{'descr': '" + dtype + "', 'fortran_order': False, 'shape': (" + forma.join(', ') + (forma.length === 1 ? ',' : '') + '), }';
    const pad = (64 - ((10 + cab.length + 1) % 64)) % 64;
    cab = cab + ' '.repeat(pad) + '\n';
    const h = Buffer.alloc(10);
    h[0] = 0x93; h.write('NUMPY', 1, 'latin1'); h[6] = 1; h[7] = 0; h.writeUInt16LE(cab.length, 8);
    const corpo = Buffer.alloc(valores.length * tam);
    valores.forEach((v, i) => {
      if (dtype === '<f8') corpo.writeDoubleLE(v, i * 8);
      else if (dtype === '<f4') corpo.writeFloatLE(v, i * 4);
      else if (dtype === '<i8') corpo.writeBigInt64LE(BigInt(v), i * 8);
      else corpo.writeInt32LE(v, i * 4);
    });
    return Buffer.concat([h, Buffer.from(cab, 'latin1'), corpo]);
  }
  function zipSintetico(arquivos) {
    const locais = [], centrais = [];
    let off = 0, k = 0;
    for (const [nome, dados] of Object.entries(arquivos)) {
      const metodo = k++ % 2 ? 8 : 0;
      const comp = metodo === 8 ? zlib.deflateRawSync(dados) : dados;
      const nb = Buffer.from(nome);
      const lh = Buffer.alloc(30);
      lh.writeUInt32LE(0x04034b50, 0); lh.writeUInt16LE(20, 4); lh.writeUInt16LE(metodo, 8);
      lh.writeUInt32LE(comp.length, 18); lh.writeUInt32LE(dados.length, 22); lh.writeUInt16LE(nb.length, 26);
      const ch = Buffer.alloc(46);
      ch.writeUInt32LE(0x02014b50, 0); ch.writeUInt16LE(20, 4); ch.writeUInt16LE(20, 6); ch.writeUInt16LE(metodo, 10);
      ch.writeUInt32LE(comp.length, 20); ch.writeUInt32LE(dados.length, 24); ch.writeUInt16LE(nb.length, 28); ch.writeUInt32LE(off, 42);
      locais.push(lh, nb, comp);
      centrais.push(ch, nb);
      off += 30 + nb.length + comp.length;
    }
    const cd = Buffer.concat(centrais);
    const fim = Buffer.alloc(22);
    fim.writeUInt32LE(0x06054b50, 0); fim.writeUInt16LE(k, 8); fim.writeUInt16LE(k, 10); fim.writeUInt32LE(cd.length, 12); fim.writeUInt32LE(off, 16);
    return Buffer.concat([...locais, cd, fim]);
  }
  function fitsSintetico(linhas) {
    const card = (k, v) => {
      const s = typeof v === 'string' && v !== 'T' && v !== 'F' ? k.padEnd(8) + '= ' + ("'" + v.padEnd(8) + "'").padEnd(20) : k.padEnd(8) + '= ' + String(v).padStart(20);
      return s.padEnd(80);
    };
    const bloco = (cards) => { let t = cards.join('') + 'END'.padEnd(80); while (t.length % 2880) t += ' '; return Buffer.from(t, 'latin1'); };
    const largura = 16;
    const prim = bloco([card('SIMPLE', 'T'), card('BITPIX', 8), card('NAXIS', 0), card('EXTEND', 'T')]);
    const ext = bloco([card('XTENSION', 'BINTABLE'), card('BITPIX', 8), card('NAXIS', 2), card('NAXIS1', largura), card('NAXIS2', linhas.length), card('PCOUNT', 0), card('GCOUNT', 1), card('TFIELDS', 3),
      card('TTYPE1', 'Z'), card('TFORM1', 'D'), card('TTYPE2', 'N'), card('TFORM2', 'J'), card('TTYPE3', 'NOME'), card('TFORM3', '4A'), card('EXTNAME', 'TESTE')]);
    const dados = Buffer.alloc(Math.ceil((largura * linhas.length) / 2880) * 2880);
    linhas.forEach(([z, n, nome], r) => { dados.writeDoubleBE(z, r * largura); dados.writeInt32BE(n, r * largura + 8); dados.write(nome.padEnd(4, '\0'), r * largura + 12, 'latin1'); });
    return Buffer.concat([prim, ext, dados]);
  }
  const barramentoTeste = () => {
    const quadros = [];
    return { quadros, enviar(tipo, de, para, payload) { const hex = URB1.enquadrar(tipo, JSON.stringify({ de, para, ...payload })); quadros.push(hex); return hex; }, receber(hex) { return JSON.parse(URB1.desenquadrar(hex).payload); } };
  };
  async function http1(base, rota, opcoes = {}) {
    const r = await fetch(base + rota, { ...opcoes, signal: AbortSignal.timeout(20000) });
    const t = await r.text();
    let j = null;
    try { j = JSON.parse(t); } catch (e) { /* html */ }
    return { status: r.status, headers: r.headers, texto: t, json: j };
  }

  /* ══ --teste ══════════════════════════════════════════════════════════ */
  async function offline() {
    console.log('PARALLAX · Células ' + VERSAO + ' — autoteste offline (Node ' + process.version + ')');
    CFG.MODO = 'simulado';
    CFG.SIMULAR_ALUCINACAO = '';
    CFG.DATA_DIR = tmpDir('teste');
    const casos = [];
    const c = (nome, f) => casos.push([nome, f]);

    c('URB1 · CRC16-CCITT("123456789") = 0x29B1', () => { const v = URB1.crc16(Buffer.from('123456789')); afirmar(v === 0x29b1, 'obtido 0x' + v.toString(16)); });
    c('URB1 · ida e volta com UTF-8', () => {
      const hex = URB1.enquadrar(URB1.TIPO.GOSSIP, '{"d":"Ω_m ✓"}');
      afirmar(hex.startsWith('55524231'), 'magic');
      const q = URB1.desenquadrar(hex);
      afirmar(q.tipo === 7 && q.versao === 1 && q.payload === '{"d":"Ω_m ✓"}', 'conteúdo');
    });
    c('URB1 · rejeita CRC, magic, comprimento e hex inválidos', () => {
      const hex = URB1.enquadrar(URB1.TIPO.MEDIDA, '{"valor":0.3}');
      const i = 20, troca = hex[i] === '0' ? '1' : '0';
      lanca(() => URB1.desenquadrar(hex.slice(0, i) + troca + hex.slice(i + 1)), /CRC/, 'CRC');
      lanca(() => URB1.desenquadrar('00' + hex.slice(2)), /magic/, 'magic');
      lanca(() => URB1.desenquadrar(hex.slice(0, -4)), /comprimento|curto/, 'comprimento');
      lanca(() => URB1.desenquadrar(hex + 'A'), /hex/, 'hex ímpar');
    });
    c('Álgebra · Cholesky, soluções triangulares e log-determinante', () => {
      const n = 6, r = U.rng(3), B = Array.from({ length: n * n }, () => r() - 0.5), A = new Float64Array(n * n);
      for (let i = 0; i < n; i++) for (let j = 0; j < n; j++) { let s = i === j ? n : 0; for (let k = 0; k < n; k++) s += B[k * n + i] * B[k * n + j]; A[i * n + j] = s; }
      const L = ALG.cholesky(A, n), b = Float64Array.from({ length: n }, (_, i) => i + 1);
      const x = ALG.resolverSuperiorT(L, n, ALG.resolverInferior(L, n, b)), Ax = ALG.matVec(A, n, x);
      for (let i = 0; i < n; i++) perto(Ax[i], b[i], 1e-10, 'A·x');
      const M = Array.from({ length: n }, (_, i) => Array.from({ length: n }, (_, j) => A[i * n + j]));
      const inv = ALG.inversaPequena(M);
      for (let i = 0; i < n; i++) for (let j = 0; j < n; j++) { let s = 0; for (let k = 0; k < n; k++) s += M[i][k] * inv[k][j]; perto(s, i === j ? 1 : 0, 1e-10, 'M·M⁻¹'); }
      const D = new Float64Array(9); D[0] = 2; D[4] = 3; D[8] = 5;
      perto(ALG.logDetCholesky(ALG.cholesky(D, 3), 3), Math.log(30), 1e-12, 'log det');
      lanca(() => ALG.cholesky(Float64Array.from([1, 2, 2, 1]), 2), /positiva definida/, 'não positiva');
    });
    c('Otimização · Nelder–Mead + Laplace recuperam mínimo e covariância de um χ² quadrático', () => {
      const mu = [0.3, 70], C = [[0.0004, 0.006], [0.006, 1.0]], W = ALG.inversaPequena(C);
      const chi2 = (x) => { const d = [x[0] - mu[0], x[1] - mu[1]]; return d[0] * (W[0][0] * d[0] + W[0][1] * d[1]) + d[1] * (W[1][0] * d[0] + W[1][1] * d[1]); };
      const r = ALG.minimizar(chi2, [0.25, 65], [0.05, 2], { maxIter: 3000, tolAbs: 1e-12 });
      perto(r.x[0], 0.3, 1e-4, 'x0'); perto(r.x[1], 70, 1e-3, 'x1');
      const cov = ALG.covarianciaLaplace(chi2, r.x, [0.005, 0.1]);
      for (let i = 0; i < 2; i++) for (let j = 0; j < 2; j++) perto(cov[i][j], C[i][j], 0.01 * Math.sqrt(C[i][i] * C[j][j]), 'cov');
    });
    c('Otimização · Rosenbrock (vale estreito) com reinícios', () => {
      const f = (x) => 100 * (x[1] - x[0] * x[0]) ** 2 + (1 - x[0]) ** 2;
      const r = ALG.minimizar(f, [-1.2, 1], [0.5, 0.5], { maxIter: 6000, tolAbs: 1e-14, reinicios: 6 });
      perto(r.x[0], 1, 1e-3, 'x'); perto(r.x[1], 1, 2e-3, 'y');
    });
    c('MCMC · Metropolis recupera média e desvio de uma gaussiana 2D correlacionada', async () => {
      const mu = [1, -2], C = [[1, 0.6], [0.6, 2]], W = ALG.inversaPequena(C);
      const lp = (x) => { const d = [x[0] - mu[0], x[1] - mu[1]]; return -0.5 * (d[0] * (W[0][0] * d[0] + W[0][1] * d[1]) + d[1] * (W[1][0] * d[0] + W[1][1] * d[1])); };
      const r = await ALG.mcmc(lp, [0, 0], C, { passos: 40000, semente: 11 });
      for (let i = 0; i < 2; i++) { const s = Math.sqrt(C[i][i]); perto(r.resumo[i].media, mu[i], 0.1 * s, 'média ' + i); perto(r.resumo[i].desvio, s, 0.1 * s, 'desvio ' + i); }
      return 'aceitação ' + U.fmt(r.aceitacao, 2);
    });
    c('Estatística · p-valores e conversão para σ', () => {
      perto(ALG.pQuiQuadrado(3.841458820694124, 1), 0.05, 1e-9, 'p(3,84; 1)');
      perto(ALG.pQuiQuadrado(10, 10), Math.exp(-5) * (1 + 5 + 12.5 + 125 / 6 + 625 / 24), 1e-12, 'Q(5,5) em forma fechada');
      perto(ALG.erfc(1), 0.157299207050285, 1e-12, 'erfc(1)');
      perto(ALG.sigmaDeP(ALG.erfc(2 / Math.SQRT2)), 2, 1e-9, 'σ de p(2σ)');
      const d = ALG.descrever([4, 1, 3, 2]); afirmar(d.n === 4 && d.media === 2.5 && d.mediana === 2.5 && d.min === 1 && d.max === 4, 'descrever');
    });
    c('Cosmologia · Einstein–de Sitter: χ(z) = 2(1 − 1/√(1+z))', () => {
      const t = COSMO.tabela({ Om: 1 }, 3, 0.005);
      for (const z of [0.1, 0.5, 1, 2.33, 2.9]) perto(COSMO.chi(t, z), 2 * (1 - 1 / Math.sqrt(1 + z)), 1e-9, 'z=' + z);
    });
    c('Cosmologia · Milne (Ω_k = 1): D_M = sinh(ln(1+z))', () => {
      const t = COSMO.tabela({ Om: 0, Ok: 1 }, 3, 0.005);
      for (const z of [0.3, 1, 2.5]) perto(COSMO.transversal(t, z), ((1 + z) - 1 / (1 + z)) / 2, 1e-9, 'z=' + z);
    });
    c('Cosmologia · CPL contra a integral de w(a) e casos-limite', () => {
      afirmar(COSMO.E2(0.7, { Om: 0.3, w0: -1, wa: 0 }) === COSMO.E2(0.7, { Om: 0.3 }), 'w0=−1, wa=0 deve reproduzir ΛCDM');
      perto(COSMO.E2(0.7, { Om: 0.3, w0: -0.9, wa: 0 }), 0.3 * 1.7 ** 3 + 0.7 * 1.7 ** (3 * 0.1), 1e-12, 'w constante');
      const w0 = -0.8, wa = -0.5, z = 1.2, a = 1 / (1 + z);
      let s = 0; const n = 20000, h = (1 - a) / n; // ∫_a^1 3(1+w(x))/x dx por Simpson
      for (let i = 0; i <= n; i++) { const x = a + i * h, f = 3 * (1 + w0 + wa * (1 - x)) / x; s += (i === 0 || i === n ? 1 : i % 2 ? 4 : 2) * f; }
      const fde = Math.exp((h / 3) * s);
      perto(COSMO.E2(z, { Om: 0.3, w0, wa }), 0.3 * (1 + z) ** 3 + 0.7 * fde, 1e-9, 'CPL');
    });
    c('Cosmologia · idade: ΛCDM analítica = integral numérica; EdS = 2/(3H₀)', () => {
      perto(COSMO.idadeNumerica(70, { Om: 0.3 }) / COSMO.idadeLCDM(70, 0.3), 1, 1e-6, 'ΛCDM');
      perto(COSMO.idadeNumerica(70, { Om: 1 }) / ((2 / 3) * COSMO.TEMPO_HUBBLE_H1_GYR / 0.7), 1, 1e-6, 'EdS');
    });
    c('Cosmologia · BAO: D_V = (z·D_M²·D_H)^(1/3) e lei de Hubble em z → 0', () => {
      const t = COSMO.tabela({ Om: 0.3 }, 2.5, 0.005);
      const dm = COSMO.baoRazao('DM_over_rs', 0.51, t, 100), dh = COSMO.baoRazao('DH_over_rs', 0.51, t, 100);
      perto(COSMO.baoRazao('DV_over_rs', 0.51, t, 100), Math.cbrt(0.51 * dm * dm * dh), 1e-10, 'D_V');
      perto(COSMO.modulo(1e-4, 1e-4, t, 70), 5 * Math.log10(COSMO.C_KMS * 1e-4 / 70) + 25, 1e-3, 'μ(z→0)');
    });
    c('Cosmologia · r_d de Aubourg 2015 contra o r_drag publicado pelo Planck 2018 VI (Tabela 2)', () => {
      // aanda.org/articles/aa/full_html/2020/09/aa33910-18/T2.html, conferido em 16/out/2026
      perto(COSMO.rdAubourg(0.02237, 0.02237 + 0.1200), 147.09, 0.05, 'TT,TE,EE+lowE+lensing');
      perto(COSMO.rdAubourg(0.02236, 0.02236 + 0.1202), 147.05, 0.05, 'TT,TE,EE+lowE');
      perto(COSMO.rdAubourg(0.02242, 0.02242 + 0.11933), 147.21, 0.05, '+BAO');
    });
    c('Leitores · NPY (f8, f4, i8) e NPZ (stored + deflate)', () => {
      const a = DADOS.npy(npySintetico('<f8', [2, 2], [1.5, -2, 3e-5, 4]));
      afirmar(a.forma.join('x') === '2x2' && a.dados[2] === 3e-5, 'f8');
      const b = DADOS.npy(npySintetico('<f4', [3], [0.25, 1, -8]));
      afirmar(b.dados[0] === 0.25 && b.dados[2] === -8, 'f4');
      const z = DADOS.npz(zipSintetico({ 'nsn.npy': npySintetico('<i8', [1], [3]), 'cov.npy': npySintetico('<f4', [6], [1, 0.5, 0.25, 2, 0.5, 3]) }));
      afirmar(z.nsn.dados[0] === 3 && z.cov.dados.length === 6 && z.cov.dados[5] === 3, 'npz');
    });
    c('Leitores · FITS BINTABLE (cru e gzip)', () => {
      const linhas = [[0.5, 7, 'ab'], [1.25, -3, 'cdef'], [2, 1000, 'x']];
      for (const buf of [fitsSintetico(linhas), zlib.gzipSync(fitsSintetico(linhas))]) {
        const t = DADOS.fits(buf);
        afirmar(t.colunas.join(',') === 'Z,N,NOME' && t.extensao === 'TESTE', 'colunas ' + t.colunas);
        afirmar(JSON.stringify(t.linhas) === JSON.stringify(linhas), 'linhas ' + JSON.stringify(t.linhas));
      }
    });
    c('Leitores · limite de linhas em FITS e CSV (arquivos de índice grandes não viram memória)', () => {
      const muitas = Array.from({ length: 1200 }, (_, i) => [i / 10, i, 'r' + (i % 7)]);
      const f = DADOS.fits(fitsSintetico(muitas), 1000);
      afirmar(f.linhas.length === 1000 && f.truncado === 1000 && f.linhasNoArquivo === 1200, 'fits ' + f.linhas.length + '/' + f.linhasNoArquivo);
      afirmar(DADOS.fits(fitsSintetico(muitas)).linhas.length === 1200, 'fits sem limite');
      const t = 'a,b\n' + muitas.map((l) => l[0] + ',' + l[1]).join('\n') + '\n';
      const c1 = DADOS.csv(t, ',', 1000);
      afirmar(c1.linhas.length === 1000 && c1.truncado === 1000, 'csv ' + c1.linhas.length);
      afirmar(DADOS.csv(t).linhas.length === 1200 && !DADOS.csv(t).truncado, 'csv sem limite');
    });
    c('Leitores · CSV com aspas, VOTable (dados e erro), SNANA, espaços, covariância, índice', () => {
      const csv = DADOS.csv('a,b,c\n1,"x,y",2.5e3\n"q""uote",,-4\n');
      afirmar(JSON.stringify(csv) === JSON.stringify({ colunas: ['a', 'b', 'c'], linhas: [[1, 'x,y', 2500], ['q"uote', null, -4]] }), 'csv ' + JSON.stringify(csv));
      const vo = DADOS.votable('<?xml version="1.0"?><VOTABLE><RESOURCE><INFO name="QUERY_STATUS" value="OK"/><TABLE><FIELD name="ra" datatype="double"/><FIELD name="nome" datatype="char"/><DATA><TABLEDATA><TR><TD>10.5</TD><TD>a&amp;b</TD></TR><TR><TD>-3</TD><TD/></TR></TABLEDATA></DATA></TABLE></RESOURCE></VOTABLE>');
      afirmar(JSON.stringify(vo) === JSON.stringify({ colunas: ['ra', 'nome'], linhas: [[10.5, 'a&b'], [-3, null]] }), 'votable ' + JSON.stringify(vo));
      lanca(() => DADOS.votable('<VOTABLE><RESOURCE><INFO name="QUERY_STATUS" value="ERROR">Tabela inexistente</INFO></RESOURCE></VOTABLE>'), /Tabela inexistente/, 'votable erro');
      const sn = DADOS.snana('# x\nVARNAMES: CID zHD MU\nSN: 1 0.1 38.2\nSN: 2 0.5 42.3\n');
      afirmar(sn.colunas.join() === 'CID,zHD,MU' && sn.linhas[1][2] === 42.3, 'snana');
      const es = DADOS.espacos('# c\nzHD zHEL m_b_corr\n0.01 0.011 14.5\n');
      afirmar(es.colunas[2] === 'm_b_corr' && es.linhas[0][2] === 14.5, 'espaços');
      const cv = DADOS.covPantheon('2\n1.0\n0.5\n0.5\n2.0\n');
      afirmar(cv.n === 2 && cv.C[3] === 2, 'cov');
      lanca(() => DADOS.covPantheon('2\n1 2 3'), /esperados 4/, 'cov curta');
      lanca(() => DADOS.covPantheon('1\n1 2'), /mais de/, 'cov longa');
      const ix = DADOS.indice('<a href="cat1.fits">x</a><a href="/abs">y</a><a href="../up">z</a><a href="https://outro/x">w</a><a href="sub/cat2.fits.gz">v</a>', 'https://erosita.mpe.mpg.de/dr1/A/');
      afirmar(JSON.stringify(ix) === JSON.stringify(['https://erosita.mpe.mpg.de/dr1/A/cat1.fits', 'https://erosita.mpe.mpg.de/dr1/A/sub/cat2.fits.gz']), 'índice ' + JSON.stringify(ix));
    });
    c('Guarda · número com registro passa; número inventado e id inexistente são violação', () => {
      LEDGER.limpar();
      const e = LEDGER.registrar({ tag: 'C', celula: 'desi', grandeza: 'Om', valor: 0.29716, sigma: 0.00862, n: 1657, numeros: [10.283] });
      const w = LEDGER.registrar({ tag: 'C', celula: 'desi', grandeza: 'w0', valor: -0.845, sigma: 0.07 });
      const esc = FERR.escopoDe('desi');
      const g1 = LEDGER.guarda('Ω_m = 0,297 ± 0,009 [' + e.id + '], χ² = 10,28 com 13 pontos; w₀ = −0,845 [' + w.id + ']', esc);
      afirmar(g1.violacoes.length === 0 && g1.conferidos === 4, 'g1 ' + JSON.stringify(g1));
      const g2 = LEDGER.guarda('Ω_m = 0,412 ± 0,003 segundo a calibração interna', esc);
      afirmar(g2.violacoes.length === 1 && g2.violacoes[0].numero === '0,412 ± 0,003', 'g2 ' + JSON.stringify(g2.violacoes));
      afirmar(LEDGER.guarda('3 rodadas em 2026, 17 células', esc).violacoes.length === 0, 'isentos');
      const g4 = LEDGER.guarda('ver [C-deadbeef]', esc);
      afirmar(g4.violacoes.length === 1 && /não existe/.test(g4.violacoes[0].trecho), 'id inexistente');
      afirmar(LEDGER.guarda('Omega_m = 0.297', esc).violacoes.length === 0, 'decimal com ponto');
      afirmar(LEDGER.guarda('1.657 SNe; 1,657 SNe', esc).violacoes.length === 0, 'milhar pt-BR e inglês');
      afirmar(LEDGER.guarda('Ω_m = 0,31', esc).violacoes.length === 1, '0,31 não arredonda 0,29716');
      afirmar(LEDGER.guarda('Ω_m = 0,30', esc).violacoes.length === 0, '0,30 arredonda 0,29716');
      afirmar(LEDGER.guarda('intervalo de 95% e 68 %', esc).violacoes.length === 0, 'nível de confiança');
      afirmar(LEDGER.guarda('foram 95 galáxias', esc).violacoes.length === 1, '95 sem % não é nível de confiança');
      const t = LEDGER.registrar({ tag: 'D', celula: 'desi', grandeza: 'theta100', valor: 1.0411, sigma: 0.0003, rotulo: '100θ*' });
      const g5 = LEDGER.guarda('100θ* = 1,0411 ± 0,0003 [' + t.id + ']; análise 3x2pt do DR2 em C01, Lyα', esc);
      afirmar(g5.violacoes.length === 0 && g5.conferidos === 2 && g5.total === 2, 'rótulos com dígitos não são números: ' + JSON.stringify(g5));
      LEDGER.registrar({ tag: 'C', celula: 'desi', grandeza: 'tensao', valor: 5.4, unidade: 'σ' });
      afirmar(LEDGER.guarda('tensão de 5,4σ', FERR.escopoDe('desi')).conferidos === 1, 'σ colado ao número');
      afirmar(LEDGER.guarda('tensão de 5,5σ', FERR.escopoDe('desi')).violacoes.length === 1, 'σ colado, número errado');
    });
    c('Guarda · prefixo de SHA-256 só com dígitos, ids estritos e sem caixa, gravidade por tipo de violação', () => {
      LEDGER.limpar();
      const e = LEDGER.registrar({ tag: 'C', celula: 'desi', grandeza: 'Om', valor: 0.3, sigma: 0.01 });
      const esc = FERR.escopoDe('desi');
      afirmar(LEDGER.guarda('arquivo X, SHA-256 20261016; hash: 4471', esc).violacoes.length === 0, 'prefixo de hash');
      afirmar(LEDGER.guarda('nota 20261016', esc).violacoes.length === 1, 'número solto parecido com hash ainda é conferido');
      afirmar(LEDGER.guarda('o F-score e o C-type não são ids', esc).violacoes.length === 0, 'F-score/C-type');
      afirmar(LEDGER.guarda('ver [' + e.id.toUpperCase() + ']', esc).violacoes.length === 0, 'id em maiúsculas resolve');
      afirmar(LEDGER.guarda('ver [C-00000000]', esc).violacoes.length === 1, 'id com forma válida mas inexistente');
      afirmar(LEDGER.guarda('ver [F-constantes]', { entradas: [], extras: [] }).violacoes.length === 1, 'F-constantes sem carregar a literatura é inexistente');
      const g = LEDGER.guarda('Ω_m = 0,41 ± 0,02 e também 0,77 e [C-deadbeef]', esc);
      afirmar(g.violacoes.length === 3 && g.gravidade === 2, 'gravidade ' + g.gravidade + ' ' + JSON.stringify(g.violacoes.map((v) => v.gravidade)));
    });
    c('Ledger · literatura atribuída a duas células: a segunda publicação vira cópia própria', () => {
      LEDGER.limpar(); LIT.carregar();
      const r22 = LEDGER.listar({ grandeza: 'H0' }).find((x) => (x.celulas || []).includes('gaia') && (x.celulas || []).includes('hubble'));
      afirmar(r22, 'SH0ES R22 atribuído a hubble e gaia');
      const a = LEDGER.publicar(r22.id, 'gaia');
      afirmar(a.id === r22.id && a.publicado_por === 'gaia', 'primeira publicação em cima do registro');
      const b = LEDGER.publicar(r22.id, 'hubble');
      afirmar(b.id !== r22.id && b.copia_de === r22.id && b.publicado_por === 'hubble' && b.valor === r22.valor && r22.publicado_por === 'gaia', 'cópia para a segunda célula');
      afirmar(LEDGER.publicar(r22.id, 'hubble').id === b.id, 'republicar devolve a mesma cópia');
      const t = POLOV.tabelaRede().H0;
      afirmar(t.medidas.filter((m) => m.celula === 'gaia').length === 1 && t.medidas.filter((m) => m.celula === 'hubble').length === 1, 'as duas na rede');
      afirmar(t.pares.some((p) => p.celulas.includes('gaia') && p.celulas.includes('hubble') && !p.independentes && p.sigma === 0), 'par marcado como não independente');
      afirmar(POLOV.identidades('rede').length === LEDGER.listar({}).filter((x) => x.fonte && x.fonte.descricao === 'Polo V · identidade').length, 'identidades não duplicam por cópia');
    });
    c('Guarda · valor ± incerteza precisa vir de um mesmo registro; erro assimétrico conferido dos dois lados', () => {
      LEDGER.limpar();
      const a = LEDGER.registrar({ tag: 'C', celula: 'desi', grandeza: 'Om', valor: 0.30, sigma: 0.01 });
      LEDGER.registrar({ tag: 'C', celula: 'desi', grandeza: 'Om', valor: 0.33, sigma: 0.02 });
      const s = LEDGER.registrar({ tag: 'D', celula: 'subaru', grandeza: 'S8', valor: 0.776, sigma_mais: 0.032, sigma_menos: 0.033, fonte: { tipo: 'literatura', url: 'https://arxiv.org/abs/2304.00701' } });
      const esc = FERR.escopoDe('desi');
      afirmar(LEDGER.guarda('Ω_m = 0,30 ± 0,01 [' + a.id + ']', esc).violacoes.length === 0, 'par do mesmo registro');
      const mist = LEDGER.guarda('Ω_m = 0,30 ± 0,02', esc);
      afirmar(mist.violacoes.length === 1 && /mesmo registro/.test(mist.violacoes[0].motivo), 'par misturando registros: ' + JSON.stringify(mist));
      afirmar(LEDGER.guarda('S₈ = 0,776 (+0,032/−0,033) [' + s.id + ']', esc).violacoes.length === 0, 'assimétrico com parênteses');
      afirmar(LEDGER.guarda('S₈ = 0,776 +0,032/−0,033', esc).violacoes.length === 0, 'assimétrico sem parênteses');
      afirmar(LEDGER.guarda('S₈ = 0,776 +0,032/−0,099', esc).violacoes.length === 1, 'σ₋ inventado');
      afirmar(LEDGER.guarda('HSC Y3 (arXiv 2304.00701)', esc).violacoes.length === 0, 'identificador arXiv da fonte');
      afirmar(LEDGER.guarda('rodou em 1432 ms', { entradas: [], extras: LEDGER.numerosDeJSON({ ms: 1432, url: 'https://x/abc/123' }) }).violacoes.length === 0, 'número devolvido por ferramenta');
      afirmar(LEDGER.numerosDeJSON({ url: 'https://raw.githubusercontent.com/a/b/c9a4fcafc4cbd19bd750dee47fc76194a45c181f/x.csv' }).length === 0, 'hash de URL não vira número');
    });
    c('Ledger · regras de publicação', () => {
      LEDGER.limpar();
      const a = LEDGER.registrar({ tag: 'C', celula: 'desi', grandeza: 'Om', valor: 0.3, sigma: 0.01, dados_usados: ['desi'], modelo: 'ΛCDM plano' });
      lanca(() => LEDGER.publicar(LEDGER.registrar({ tag: 'C', celula: 'desi', grandeza: 'Om', valor: 0.3 }).id, 'desi'), /incerteza/, 'sem σ');
      lanca(() => LEDGER.publicar(a.id, 'des'), /só publica registros próprios/, 'célula alheia');
      lanca(() => LEDGER.publicar(LEDGER.registrar({ tag: 'C', celula: 'desi', grandeza: 'tensao', valor: 2, sigma: 1 }).id, 'desi'), /grandezas cosmológicas/, 'grandeza');
      lanca(() => LEDGER.publicar(LEDGER.registrar({ tag: 'F', celula: 'desi', grandeza: 'Om', valor: 0.3, sigma: 0.1 }).id, 'desi'), /\[C\] ou \[D\]/, 'etiqueta F');
      lanca(() => LEDGER.publicar('C-00000000', 'desi'), /inexistente/, 'id');
      const p = LEDGER.publicar(a.id, 'desi');
      afirmar(p.publicado && p.publicado_por === 'desi', 'publicado');
    });
    c('Polo V · tensão assimétrica, combinação, independência e pontes', () => {
      LEDGER.limpar();
      const a = LEDGER.registrar({ tag: 'C', celula: 'desi', grandeza: 'Om', valor: 0.3, sigma: 0.01, dados_usados: ['desi'], modelo: 'ΛCDM plano' });
      const b = LEDGER.registrar({ tag: 'C', celula: 'des', grandeza: 'Om', valor: 0.33, sigma_mais: 0.02, sigma_menos: 0.01, dados_usados: ['des-sn'], modelo: 'ΛCDM plano' });
      const t = POLOV.tensao(a.id, b.id);
      perto(t.valor, 0.03 / Math.sqrt(0.0002), 1e-12, 'tensão com σ do lado certo');
      afirmar(!t.avisos.length, 'sem avisos');
      const x = LEDGER.registrar({ tag: 'C', celula: 'hubble', grandeza: 'Om', valor: 0.31, sigma: 0.02, dados_usados: ['des-sn', 'pantheon+'], modelo: 'ΛCDM plano' });
      lanca(() => POLOV.combinar([b.id, x.id]), /não são independentes/, 'combinação dependente');
      afirmar(POLOV.tensao(b.id, x.id).avisos.some((s) => /não independentes/.test(s)), 'aviso de dependência');
      const cb = POLOV.combinar([a.id, b.id]);
      const w1 = 1 / 0.0001, w2 = 1 / 0.015 ** 2;
      perto(cb.valor, (w1 * 0.3 + w2 * 0.33) / (w1 + w2), 1e-12, 'média'); perto(cb.sigma, 1 / Math.sqrt(w1 + w2), 1e-12, 'σ');
      const h = LEDGER.registrar({ tag: 'C', celula: 'desi', grandeza: 'hrd', valor: 101.5, sigma: 0.7, dados_usados: ['desi'], modelo: 'ΛCDM plano' });
      const hd = LEDGER.registrar({ tag: 'C', celula: 'desi', grandeza: 'H0', valor: 68, sigma: 1, dados_usados: ['desi'] });
      lanca(() => POLOV.rdTardio(h.id, hd.id), /recusado/, 'r_d tardio com dados comuns');
      const ob = LEDGER.registrar({ tag: 'D', celula: 'planck', grandeza: 'ombh2', valor: 0.02237, sigma: 0.00015, dados_usados: ['planck'], fonte: { tipo: 'literatura', descricao: 'teste' } });
      const oc = LEDGER.registrar({ tag: 'D', celula: 'planck', grandeza: 'omch2', valor: 0.1200, sigma: 0.0012, dados_usados: ['planck'], fonte: { tipo: 'literatura', descricao: 'teste' } });
      const rd = POLOV.rdCMB(ob.id, oc.id);
      perto(rd.valor, 147.09, 0.05, 'r_d CMB');
      const h0 = POLOV.h0ReguaInversa(h.id, rd.id);
      perto(h0.valor, 100 * 101.5 / rd.valor, 1e-12, 'H₀ régua inversa');
      lanca(() => POLOV.tensao(a.id, h.id), /grandezas diferentes/, 'grandezas');
    });
    c('Polo V · identidade Ω_m = (ω_b+ω_c+ω_ν)/h² nos valores do resumo do Planck 2018', () => {
      LEDGER.limpar(); LIT.carregar();
      const ids = POLOV.identidades('rede');
      const om = ids.find((e) => /Planck 2018/.test(e.rotulo) && /Ω_m =/.test(e.rotulo));
      afirmar(om && om.ok, 'identidade do Planck ' + JSON.stringify(om && { valor: om.valor, numeros: om.numeros }));
      return 'Ω_m calculado ' + U.fmt(om.valor, 4) + ', diferença ' + U.fmt(om.numeros[0], 2) + 'σ';
    });
    c('Literatura · ids estáveis entre recargas', () => {
      LEDGER.limpar(); LIT.carregar();
      const a = LEDGER.listar({}).filter((e) => e.tag === 'D').map((e) => e.id).sort().join();
      LEDGER.limpar(); LIT.carregar();
      const b = LEDGER.listar({}).filter((e) => e.tag === 'D').map((e) => e.id).sort().join();
      afirmar(a === b && a.length > 0, 'ids mudaram');
      afirmar(new Set(LEDGER.listar({}).map((e) => e.id)).size === LEDGER.listar({}).length, 'ids repetidos');
    });
    c('Ferramentas · permissão por papel e publicar_medida copia o valor do registro', async () => {
      LEDGER.limpar(); LIT.carregar();
      const bar = barramentoTeste();
      const cel = CELULAS.porId.planck;
      const neg = await FERR.executar('publicar_medida', { id: 'x' }, { celula: cel, permitidas: ['ledger'], barramento: bar });
      afirmar(/não permitida/.test(neg.erro), 'permissão');
      const lit = LEDGER.listar({ celula: 'planck', grandeza: 'H0' })[0];
      const r = await FERR.executar('publicar_medida', { id: lit.id, valor: 99 }, { celula: cel, permitidas: ['publicar_medida'], barramento: bar });
      afirmar(!r.erro, r.erro);
      const q = bar.receber(bar.quadros[0]);
      afirmar(q.valor === lit.valor && q.valor !== 99 && q.id === lit.id, 'valor do quadro MEDIDA');
      const ruim = await FERR.executar('consultar_tap', { servico: 'gaia', adql: 'SELECT 1' }, { celula: cel, permitidas: ['consultar_tap'] });
      afirmar(/não permitido/.test(ruim.erro), 'TAP fora da célula');
      await TAP.consultar('gaia', 'DELETE FROM x').then(() => { throw new Error('TAP aceitou DELETE'); }, (e) => afirmar(/SELECT/.test(e.message), e.message));
    });
    c('Agente · laço tool_use → tool_result no simulador e resposta forçada no limite de passos', async () => {
      LEDGER.limpar(); LIT.carregar();
      const cel = CELULAS.porId.planck, papel = PAPEIS.porId.teorico;
      const ctx = { celula: cel, papel: 'teorico', permitidas: papel.ferramentas, sinal: new AbortController().signal, barramento: barramentoTeste(), extrasGuarda: [] };
      const sis = PAPEIS.sistema(papel, cel, { objetivo: 'teste' });
      const r = await IA.agente({ sistema: sis, usuario: 'Execute seu papel agora.', ferramentas: papel.ferramentas, ctx, esforco: 'medium', maxPassos: 8 });
      afirmar(r.passos >= 2 && r.stop === 'end_turn' && /DIGEST:/.test(r.texto), 'laço: ' + JSON.stringify({ passos: r.passos, stop: r.stop }));
      const g = LEDGER.guarda(r.texto, FERR.escopoDe('planck'));
      afirmar(g.violacoes.length === 0 && g.conferidos > 0, 'guarda ' + JSON.stringify(g));
      const r1 = await IA.agente({ sistema: sis, usuario: 'Execute seu papel agora.', ferramentas: papel.ferramentas, ctx, esforco: 'medium', maxPassos: 1 });
      afirmar(r1.passos === 1 && /DIGEST:/.test(r1.texto), 'limite de passos');
      afirmar(IA.digestDe('x\nDIGEST: a | b |  | c').join('/') === 'a/b/c', 'digest');
      return r.passos + ' rodadas de ferramenta, ' + g.conferidos + ' números conferidos';
    });
    c('Painel · HTML com nonce e script sintaticamente válido', () => {
      const h = PAINEL.html('NONCE123', true);
      afirmar(h.includes('nonce="NONCE123"') && h.includes('id="tok"') && !h.includes('${'), 'html');
      const m = /<script nonce="NONCE123">([\s\S]*?)<\/script>/.exec(h);
      afirmar(m, 'script ausente');
      new Function(m[1]); // lança SyntaxError se o JS do painel estiver quebrado
      afirmar(!PAINEL.html('n', false).includes('type="password"'), 'campo de token sem exigência');
    });

    // rotas HTTP e um ciclo de 4 células só com literatura (sem rede)
    let srv = null, base = '';
    const cfg0 = { PRODUCAO: CFG.PRODUCAO, ACCESS_TOKEN: CFG.ACCESS_TOKEN };
    c('Servidor · rotas, cabeçalhos, token, limites de corpo e SSE', async () => {
      srv = http.createServer((q, s) => SERV.rotear(q, s).catch((e) => { s.writeHead(500); s.end(String(e)); }));
      await new Promise((r) => srv.listen(0, '127.0.0.1', r));
      base = 'http://127.0.0.1:' + srv.address().port;
      LEDGER.limpar(); LIT.carregar();
      const p = await http1(base, '/');
      afirmar(p.status === 200 && /PARALLAX/.test(p.texto), 'painel');
      const csp = p.headers.get('content-security-policy') || '';
      const nonce = /nonce-([^']+)'/.exec(csp);
      afirmar(nonce && p.texto.includes('nonce="' + nonce[1] + '"'), 'nonce da CSP');
      afirmar(p.headers.get('x-content-type-options') === 'nosniff' && p.headers.get('x-frame-options') === 'DENY', 'cabeçalhos');
      afirmar((await http1(base, '/saude')).json.ok === true, 'saúde');
      const est = (await http1(base, '/api/estado')).json;
      afirmar(est.celulas.length === 17 && typeof est.seq === 'number', 'estado');
      afirmar((await http1(base, '/api/ledger?celula=planck')).json.registros.length > 0, 'ledger');
      afirmar((await http1(base, '/api/dossies/..%2F..%2Fetc%2Fpasswd')).status === 400, 'dossiê com caminho');
      afirmar((await http1(base, '/api/celula/inexistente')).status === 404, 'célula inexistente');
      afirmar((await http1(base, '/nada')).status === 404, '404');
      CFG.PRODUCAO = true; CFG.ACCESS_TOKEN = '';
      afirmar((await http1(base, '/api/ciclo', { method: 'POST', body: '{}' })).status === 503, 'produção sem token');
      CFG.ACCESS_TOKEN = 'segredo-de-teste';
      afirmar((await http1(base, '/api/ciclo', { method: 'POST', body: '{}' })).status === 401, 'sem token');
      afirmar((await http1(base, '/api/ciclo', { method: 'POST', body: '{}', headers: { authorization: 'Bearer errado' } })).status === 401, 'token errado');
      afirmar((await http1(base, '/api/testar', { headers: { authorization: 'Bearer errado' } })).status === 401, 'testar sem token');
      const grande = await http1(base, '/api/ciclo', { method: 'POST', body: JSON.stringify({ objetivo: 'x'.repeat(70000) }), headers: { authorization: 'Bearer segredo-de-teste' } });
      afirmar(grande.status === 413, 'corpo grande: ' + grande.status);
      afirmar((await http1(base, '/api/ciclo', { method: 'POST', body: '{x', headers: { authorization: 'Bearer segredo-de-teste' } })).status === 400, 'JSON inválido');
      afirmar((await http1(base, '/api/ciclo', { method: 'POST', body: '{"celulas":["vst"]}', headers: { authorization: 'Bearer segredo-de-teste' } })).status === 400, 'célula desconhecida');
      const ac = new AbortController();
      const r = await fetch(base + '/api/eventos?desde=0', { signal: ac.signal });
      afirmar(r.headers.get('content-type').startsWith('text/event-stream'), 'SSE');
      const leitor = r.body.getReader();
      const { value } = await leitor.read();
      afirmar(Buffer.from(value).toString().startsWith('retry:'), 'SSE retry');
      await leitor.cancel().catch(() => {});
      ac.abort();
      afirmar((await http1(base, '/api/celula/constructor')).status === 404 && (await http1(base, '/api/celula/__proto__')).status === 404, 'chaves herdadas não são células');
      afirmar((await http1(base, '/api/ciclo', { method: 'POST', body: '{"celulas":["constructor"]}', headers: { authorization: 'Bearer segredo-de-teste' } })).status === 400, 'ciclo com chave herdada');
      // teto de conexões SSE
      const acs = [], abertas = [];
      for (let i = 0; i < 50; i++) { const a = new AbortController(); acs.push(a); abertas.push(fetch(base + '/api/eventos', { signal: a.signal })); }
      const rs = await Promise.all(abertas);
      afirmar(rs.every((x) => x.status === 200), '50 conexões aceitas');
      await U.dormir(100);
      const ex = await http1(base, '/api/eventos');
      afirmar(ex.status === 503, '51ª conexão: ' + ex.status);
      for (const a of acs) a.abort();
      await U.dormir(200);
      afirmar((await fetch(base + '/api/eventos', { signal: AbortSignal.timeout(2000) }).then((x) => x.status).catch(() => 0)) === 200, 'conexões liberadas após fechar');
    });
    c('Ciclo simulado offline · Planck, ACT, SPT, CHIME (só literatura) pela rota /api/ciclo', async () => {
      const fim = new Promise((res) => { const f = (ev) => { if (ev.t === 'fim') { ORQ.ouvintes.delete(f); res(ev); } }; ORQ.ouvintes.add(f); });
      const r = await http1(base, '/api/ciclo', { method: 'POST', body: JSON.stringify({ celulas: ['planck', 'act', 'spt', 'chime'], rodadas: 2 }), headers: { authorization: 'Bearer segredo-de-teste', 'content-type': 'application/json' } });
      afirmar(r.status === 202 && r.json && Array.isArray(r.json.celulas) && r.json.celulas.length === 4, 'início: HTTP ' + r.status + ' ' + r.texto);
      afirmar((await http1(base, '/api/ciclo', { method: 'POST', body: '{}', headers: { authorization: 'Bearer segredo-de-teste' } })).status === 409, 'segundo ciclo simultâneo');
      const ev = await fim;
      const cc = ORQ.atual();
      afirmar(ev.estado === 'concluido', 'estado ' + ev.estado + ' ' + (ev.erro || ''));
      const erros = [], viol = [];
      for (const [id, s] of Object.entries(cc.celulas)) for (const [p, v] of Object.entries(s.papeis)) { if (v.estado === 'erro') erros.push(id + '.' + p + ': ' + v.erro); if (v.guarda && v.guarda.violacoes.length) viol.push(id + '.' + p + ': ' + JSON.stringify(v.guarda.violacoes)); }
      afirmar(!erros.length, 'erros: ' + erros.join('; '));
      afirmar(!viol.length, 'violações sem alucinação injetada: ' + viol.join('; '));
      const pub = LEDGER.listar({ publicado: true });
      afirmar(pub.some((e) => e.publicado_por === 'planck' && e.grandeza === 'H0') && pub.some((e) => e.publicado_por === 'spt'), 'publicações');
      afirmar(pub.every((e) => e.tag === 'D' || e.tag === 'C'), 'etiquetas publicadas');
      afirmar(LEDGER.listar({ grandeza: 'rd' }).some((e) => /CMB/.test(e.rotulo)), 'r_d pelo CMB na redução');
      afirmar(cc.rede.estado === 'ok', 'parecer: ' + cc.rede.estado + ' ' + (cc.rede.erro || JSON.stringify(cc.rede.guarda && cc.rede.guarda.violacoes)));
      afirmar(cc.contador.quadros > 0 && cc.contador.rejeitados === 0, 'URB1 ' + JSON.stringify(cc.contador));
      afirmar(cc.arquivo && JSON.parse(fs.readFileSync(cc.arquivo, 'utf8')).registros.length > 0, 'dossiê');
      const ds = (await http1(base, '/api/dossies')).json.dossies;
      afirmar(ds.length === 1 && (await http1(base, '/api/dossies/' + ds[0].id)).json.id === cc.id, 'rota de dossiê');
      const det = (await http1(base, '/api/celula/planck')).json;
      afirmar(det.execucoes && det.execucoes.length >= 8, 'detalhe da célula');
      return pub.length + ' medidas publicadas, ' + cc.contador.quadros + ' quadros URB1, ' + ev.duracao_s + ' s';
    });

    c('Parar no meio do ciclo · estado "interrompido", nenhum papel ativo, dossiê salvo', async () => {
      const fim = new Promise((res) => { const f = (ev) => { if (ev.t === 'fim') { ORQ.ouvintes.delete(f); res(ev); } }; ORQ.ouvintes.add(f); });
      const r = await http1(base, '/api/ciclo', { method: 'POST', body: JSON.stringify({ celulas: ['planck', 'act', 'spt', 'chime', 'meerkat'], rodadas: 1 }), headers: { authorization: 'Bearer segredo-de-teste', 'content-type': 'application/json' } });
      afirmar(r.status === 202, 'início: ' + r.texto);
      await U.dormir(60);
      const p = await http1(base, '/api/parar', { method: 'POST', headers: { authorization: 'Bearer segredo-de-teste' } });
      afirmar(p.json && p.json.parado === true, 'parar: ' + p.texto);
      const ev = await fim;
      afirmar(ev.estado === 'interrompido', 'estado ' + ev.estado);
      await U.dormir(300);
      const cc = ORQ.atual();
      const ativos = Object.values(cc.celulas).flatMap((s) => Object.values(s.papeis)).filter((v) => v.estado === 'ativo');
      afirmar(!ativos.length, ativos.length + ' papel(is) ainda ativo(s)');
      afirmar(cc.arquivo && fs.existsSync(cc.arquivo), 'dossiê do ciclo interrompido');
      afirmar((await http1(base, '/api/parar', { method: 'POST', headers: { authorization: 'Bearer segredo-de-teste' } })).json.parado === false, 'parar sem ciclo');
      return 'interrompido em ' + ev.duracao_s + ' s';
    });

    const r = await rodar(casos);
    if (srv) srv.close();
    Object.assign(CFG, cfg0);
    console.log('\n' + r.nOk + ' de ' + casos.length + ' testes passaram' + (r.falhas.length ? '; FALHARAM: ' + r.falhas.join(' · ') : ''));
    return !r.falhas.length;
  }

  /* ══ --validar ════════════════════════════════════════════════════════ */
  function lerCadeia(reg, amostrador) {
    const linhas = DADOS.texto(reg.buf).split(/\r?\n/);
    const cab = linhas[0].replace(/^#/, '').trim().split(/\s+/);
    const dados = [];
    // CosmoSIS escreve -inf/inf/nan (Number('-inf') daria NaN em JS): amostras recusadas pelo prior têm log_weight = -inf e peso 0
    const num = (x) => { const t = x.toLowerCase(); return t === '-inf' ? -Infinity : t === 'inf' || t === '+inf' ? Infinity : t === 'nan' ? NaN : Number(x); };
    for (const l of linhas) { if (!l.trim() || l[0] === '#') continue; dados.push(l.trim().split(/\s+/).map(num)); }
    let pesos, descarte = 0;
    if (amostrador === 'nautilus') {
      const iw = cab.indexOf('log_weight');
      if (iw < 0) throw new Error('cadeia nautilus sem log_weight');
      let mx = -Infinity;
      for (const d of dados) if (d[iw] > mx) mx = d[iw];
      pesos = dados.map((d) => Math.exp(d[iw] - mx));
    } else {
      descarte = Math.floor(dados.length * 0.25); // emcee: descarta os primeiros 25% como aquecimento
      dados.splice(0, descarte);
      pesos = dados.map(() => 1);
    }
    return { cab, dados, pesos, descarte, url: reg.url, sha256: reg.sha256 };
  }
  function resumoPonderado(cad, coluna, escala) {
    const i = cad.cab.indexOf(coluna);
    if (i < 0) throw new Error('coluna ' + coluna + ' ausente na cadeia');
    const pares = cad.dados.map((d, k) => [d[i] * escala, cad.pesos[k]]).filter(([x, w]) => Number.isFinite(x) && Number.isFinite(w) && w > 0).sort((a, b) => a[0] - b[0]);
    if (!pares.length) throw new Error('cadeia sem amostras com peso positivo na coluna ' + coluna);
    let W = 0, Wx = 0;
    for (const [x, w] of pares) { W += w; Wx += w * x; }
    const media = Wx / W;
    let V = 0;
    for (const [x, w] of pares) V += w * (x - media) * (x - media);
    const quantil = (p) => { let acc = 0; for (const [x, w] of pares) { acc += w; if (acc >= p * W) return x; } return pares[pares.length - 1][0]; };
    const W2 = pares.reduce((s, [, w]) => s + w * w, 0);
    return { media, desvio: Math.sqrt(V / W), mediana: quantil(0.5), p16: quantil(0.15865), p84: quantil(0.84135), nEfetivo: (W * W) / W2 };
  }
  async function validar() {
    console.log('PARALLAX · Células ' + VERSAO + ' — validação dos pipelines contra as cadeias oficiais');
    console.log('Alvos calculados agora a partir das cadeias publicadas (nada embutido). Critério: |Δ| < 0,2σ e razão de σ entre 0,8 e 1,2 (Laplace); |Δ| < 0,25σ e razão entre 0,75 e 1,25 (MCMC).\n');
    const cad = {};
    for (const [k, c] of Object.entries(CELULAS.CADEIAS)) {
      const reg = await DADOS.baixar(c.url, { maxBytes: 60e6 });
      cad[k] = lerCadeia(reg, c.amostrador);
      const zerados = cad[k].pesos.filter((w) => !(w > 0)).length;
      console.log('  cadeia ' + k + ': ' + cad[k].dados.length + ' amostras' + (cad[k].descarte ? ' (descartadas ' + cad[k].descarte + ' de aquecimento)' : '') + (zerados ? ', ' + zerados + ' com peso zero (log_weight = −inf)' : '') + ' · sha256 ' + reg.sha256.slice(0, 12) + ' · ' + c.url);
    }
    const P = 'cosmological_parameters--';
    const casos = [
      { nome: 'DESI DR2 BAO · ΛCDM plano', comp: ['bao'], modelo: 'lcdm', cadeia: 'bao_lcdm', params: [['Om', P + 'omega_m', 1]] },
      { nome: 'DES-Dovekie SN Ia · ΛCDM plano', comp: ['sn'], modelo: 'lcdm', cadeia: 'sn_lcdm', params: [['Om', P + 'omega_m', 1]] },
      { nome: 'Pantheon+SH0ES · ΛCDM plano', comp: ['shoes'], modelo: 'lcdm', cadeia: 'shoes_lcdm', params: [['Om', P + 'omega_m', 1], ['H0', P + 'h0', 100], ['M', 'supernova_params--m', 1]] },
      { nome: 'DESI DR2 BAO + DES-Dovekie · wCDM plano', comp: ['bao', 'sn'], modelo: 'wcdm', cadeia: 'bao_sn_wcdm', params: [['Om', P + 'omega_m', 1], ['w', P + 'w', 1]] },
      { nome: 'DESI DR2 BAO + DES-Dovekie · w0waCDM plano', comp: ['bao', 'sn'], modelo: 'w0wacdm', cadeia: 'bao_sn_w0wacdm', params: [['Om', P + 'omega_m', 1], ['w0', P + 'w', 1], ['wa', P + 'wa', 1]] },
    ];
    let falhas = 0;
    for (const k of casos) {
      const res = await PIPE.ajustar(k.comp, k.modelo, {});
      const mc = !!res.aceitacao;
      console.log('\n  ' + k.nome + ' — ' + res.metodo + ' · χ²min ' + U.fmt(res.chi2min, 2) + ' para ' + res.n + ' pontos · ' + U.fmt(res.ms / 1000, 1) + ' s' + (mc ? ' · aceitação ' + U.fmt(res.aceitacao, 2) : ''));
      for (const [nome, col, escala] of k.params) {
        const o = resumoPonderado(cad[k.cadeia], col, escala);
        const of = mc ? { c: o.mediana, s: (o.p84 - o.p16) / 2, txt: U.fmt(o.mediana, 4) + ' +' + U.fmt(o.p84 - o.mediana, 4) + '/−' + U.fmt(o.mediana - o.p16, 4) + ' (mediana)' }
          : { c: o.media, s: o.desvio, txt: U.fmt(o.media, 4) + ' ± ' + U.fmt(o.desvio, 4) + ' (média)' };
        let nosso, sNosso, txt;
        if (nome === 'M') { nosso = res.M_melhor; sNosso = null; txt = U.fmt(nosso, 4) + ' (mínimo analítico, sem σ próprio)'; }
        else {
          const p = res.parametros[nome];
          nosso = p.valor;
          sNosso = mc ? (p.sigma_mais + p.sigma_menos) / 2 : p.sigma;
          txt = mc ? U.fmt(p.valor, 4) + ' +' + U.fmt(p.sigma_mais, 4) + '/−' + U.fmt(p.sigma_menos, 4) : U.fmt(p.valor, 4) + ' ± ' + U.fmt(p.sigma, 4);
        }
        const d = Math.abs(nosso - of.c) / of.s;
        const razao = sNosso === null ? null : sNosso / of.s;
        const lim = mc ? [0.25, 0.75, 1.25] : [0.2, 0.8, 1.2];
        const passou = d < lim[0] && (razao === null || (razao > lim[1] && razao < lim[2]));
        if (!passou) falhas++;
        console.log('    ' + (passou ? '✓' : '✗') + ' ' + nome.padEnd(3) + ' oficial ' + of.txt + ' · aqui ' + txt + ' · |Δ| = ' + U.fmt(d, 3) + 'σ' + (razao === null ? '' : ' · σ aqui/oficial = ' + U.fmt(razao, 3)));
      }
    }
    console.log('\n' + (falhas ? falhas + ' comparação(ões) FORA do critério' : 'todas as comparações dentro do critério'));
    return !falhas;
  }

  /* ══ --simular ════════════════════════════════════════════════════════ */
  async function simular() {
    CFG.MODO = 'simulado';
    if (!CFG.SIMULAR_ALUCINACAO) CFG.SIMULAR_ALUCINACAO = 'euclid';
    const rogue = CFG.SIMULAR_ALUCINACAO;
    console.log('PARALLAX · Células ' + VERSAO + ' — ciclo completo simulado (17 células × 8 papéis), célula com número inventado: ' + rogue);
    console.log('Os papéis são o simulador determinístico; ferramentas, dados, pipelines, URB1, gossip, peso e Polo V são o código real. DATA_DIR=' + CFG.DATA_DIR + '\n');
    const logs = [];
    const fim = new Promise((res) => ORQ.ouvintes.add((ev) => {
      if (ev.t === 'log') logs.push(ev.msg);
      if (ev.t === 'ciclo' && ev.fase) console.log('  · fase ' + ev.fase);
      if (ev.t === 'gossip') console.log('    gossip rodada ' + ev.rodada + ': Δ = ' + U.fmt(ev.delta, 3) + ', ' + ev.ativas + ' células ativas');
      if (ev.t === 'fim') res(ev);
    }));
    await ORQ.executar({});
    const ev = await fim;
    const c = ORQ.atual();
    const casos = [];
    const k = (nome, f) => casos.push([nome, f]);
    k('ciclo concluído', () => afirmar(ev.estado === 'concluido', ev.estado + ' ' + (ev.erro || '')));
    k('nenhum papel terminou em erro', () => {
      const erros = [];
      for (const [id, s] of Object.entries(c.celulas)) for (const [p, v] of Object.entries(s.papeis)) if (v.estado === 'erro' || v.estado === 'aguardando' || (v.estado === 'suprimido' && !(p === 'embaixador' && s.suprimida))) erros.push(id + '.' + p + ' ' + v.estado + ': ' + v.erro);
      afirmar(!erros.length, erros.join('; '));
      return Object.keys(c.celulas).length + ' células × 8 papéis (EMBAIXADOR das suprimidas fica fora do gossip)';
    });
    k('nenhuma ferramenta devolveu erro', () => {
      const fs1 = [];
      for (const s of Object.values(c.celulas)) for (const v of Object.values(s.papeis)) for (const f of v.ferramentas || []) if (f.erro) fs1.push(s.id + '·' + f.nome + ': ' + f.erro);
      afirmar(!fs1.length, fs1.slice(0, 6).join(' | '));
    });
    k('guarda pegou o número inventado na célula ' + rogue, () => {
      const g = c.celulas[rogue].papeis.teorico.guarda;
      afirmar(g && g.violacoes.some((v) => v.numero === '0,412 ± 0,003'), JSON.stringify(g && g.violacoes));
    });
    k('peso da célula ' + rogue + ' abaixo do limiar e suprimida do gossip', () => {
      const s = c.celulas[rogue];
      afirmar(s.peso < CFG.LIMIAR_PESO && s.suprimida, 'peso ' + s.peso);
      afirmar(logs.some((m) => m.startsWith('SUPRIMIDO ' + rogue + ' → ')), 'nenhum log de supressão');
      return 'peso ' + U.fmt(s.peso, 2) + ' (' + s.motivosPeso.join(', ') + ')';
    });
    k('zero violação nas outras 16 células (sem falso positivo da guarda)', () => {
      const v = [];
      for (const [id, s] of Object.entries(c.celulas)) if (id !== rogue) for (const [p, x] of Object.entries(s.papeis)) if (x.guarda && x.guarda.violacoes.length) v.push(id + '.' + p + ': ' + JSON.stringify(x.guarda.violacoes));
      afirmar(!v.length, v.join(' | '));
    });
    k('barramento URB1: todos os quadros conferidos, nenhum rejeitado', () => { afirmar(c.contador.quadros > 0 && c.contador.rejeitados === 0, JSON.stringify(c.contador)); return c.contador.quadros + ' quadros'; });
    k('DESI, DES e Hubble publicaram medidas [C] com arquivos e SHA-256', () => {
      const pub = LEDGER.listar({ publicado: true });
      for (const [cel, g] of [['desi', 'Om'], ['desi', 'hrd'], ['des', 'Om'], ['hubble', 'H0']]) {
        const e = pub.find((x) => x.publicado_por === cel && x.grandeza === g && x.tag === 'C');
        afirmar(e, cel + ' sem ' + g);
        afirmar(e.arquivos && e.arquivos.length && e.arquivos.every((a) => /^[0-9a-f]{64}$/.test(a.sha256)), cel + ' ' + g + ' sem proveniência');
      }
    });
    k('toda medida publicada tem incerteza e etiqueta [C] ou [D]; nenhuma variante de checagem publicada', () => {
      const pub = LEDGER.listar({ publicado: true });
      afirmar(pub.every((e) => (e.tag === 'C' || e.tag === 'D') && LEDGER.sigmaSimetrico(e) > 0), 'publicação inválida');
      afirmar(!pub.some((e) => /\[checagem:/.test(e.rotulo || '')), 'variante de checagem publicada');
      afirmar(LEDGER.listar({ celula: 'des' }).some((e) => /\[checagem: covariância só estatística\]/.test(e.rotulo || '')), 'CÉTICO da DES deveria ter rodado a checagem só estatística');
    });
    k('redução: ajuste conjunto BAO+SN (ΛCDM e w0waCDM), r_d tardio, r_d CMB e H₀ pela régua inversa', () => {
      const rede = LEDGER.listar({ celula: 'rede' });
      afirmar(rede.some((e) => e.grandeza === 'Om' && e.modelo === 'ΛCDM plano' && e.tag === 'C'), 'conjunto ΛCDM');
      afirmar(rede.some((e) => e.grandeza === 'wa'), 'conjunto w0waCDM');
      afirmar(rede.some((e) => e.grandeza === 'rd' && /tardio/.test(e.rotulo)) && rede.some((e) => e.grandeza === 'rd' && /CMB/.test(e.rotulo)), 'r_d');
      afirmar(rede.some((e) => e.grandeza === 'H0' && /régua inversa/.test(e.rotulo)), 'régua inversa');
      afirmar(rede.some((e) => e.grandeza === 'tensao' && /r_d/.test(e.rotulo)), 'tensão r_d tardio × CMB');
    });
    k('nenhuma combinação misturou dados não independentes', () => {
      for (const e of LEDGER.listar({ celula: 'rede' }).filter((x) => /combinado/.test(x.rotulo || ''))) {
        const es = e.entradas.map((id) => LEDGER.obter(id));
        for (let i = 0; i < es.length; i++) for (let j = i + 1; j < es.length; j++) afirmar(!POLOV.sobrepoe(es[i], es[j]).length, e.id + ' mistura ' + es[i].id + ' e ' + es[j].id);
      }
    });
    k('parecer da rede sem violação', () => afirmar(c.rede.estado === 'ok', c.rede.estado + ' ' + JSON.stringify(c.rede.guarda && c.rede.guarda.violacoes)));
    k('gossip convergiu ou esgotou as rodadas', () => { afirmar(c.gossip.length >= 1, 'sem rodadas'); return c.gossip.map((g) => 'r' + g.rodada + ' Δ=' + U.fmt(g.delta, 3)).join(', '); });
    k('dossiê salvo e legível', () => { const d = JSON.parse(fs.readFileSync(c.arquivo, 'utf8')); afirmar(d.registros.length > 0 && d.celulasCompletas[rogue], 'dossiê'); return c.arquivo + ' (' + fs.statSync(c.arquivo).size + ' bytes)'; });

    const r = await rodar(casos);
    console.log('\n  célula        peso  supr.  viol.  publicado');
    for (const cel of c.lista) {
      const s = c.celulas[cel.id];
      const pub = LEDGER.listar({ publicado: true }).filter((e) => e.publicado_por === cel.id).map((e) => e.grandeza + '[' + e.tag + ']');
      console.log('  ' + cel.id.padEnd(12) + U.fmt(s.peso, 2).padStart(5) + '  ' + (s.suprimida ? 'sim' : 'não').padEnd(5) + '  ' + String(s.violacoes).padStart(5) + '  ' + (pub.join(' ') || '—'));
    }
    console.log('\n  rede (medidas publicadas):');
    for (const [g, t] of Object.entries(c.tabela || {})) {
      console.log('   ' + g + ': ' + t.medidas.map((m) => m.celula + ' ' + U.fmt(m.valor, U.casasPara(LEDGER.sigmaSimetrico(m))) + '±' + U.fmt(LEDGER.sigmaSimetrico(m), U.casasPara(LEDGER.sigmaSimetrico(m))) + '[' + m.tag + ']').join(' · '));
      for (const p of t.pares.filter((x) => x.sigma >= CFG.LIMIAR_TENSAO)) console.log('      tensão ' + p.celulas.join(' × ') + ': ' + U.fmt(p.sigma, 1) + 'σ' + (p.independentes ? '' : ' (não independentes)') + (p.mesmoModelo ? '' : ' (modelos diferentes)'));
    }
    const usoMem = process.resourceUsage().maxRSS / 1024;
    console.log('\n  duração ' + ev.duracao_s + ' s · chamadas simuladas ' + IA.custo.chamadas + ' · pico de memória (RSS) ' + U.fmt(usoMem, 0) + ' MB');
    console.log('  custo: o simulador não chama a API; os tokens contados são estimativa grosseira do simulador, não medida');
    console.log('\n' + r.nOk + ' de ' + casos.length + ' verificações passaram' + (r.falhas.length ? '; FALHARAM: ' + r.falhas.join(' · ') : ''));
    return !r.falhas.length;
  }

  /* ══ --simular-api ════════════════════════════════════════════════════ */
  const ESFORCOS = ['low', 'medium', 'high', 'xhigh', 'max'];
  function validarRequisicao(req, txt, chave, vistos) {
    if (req.method !== 'POST' || req.url !== '/v1/messages') return 'rota ' + req.method + ' ' + req.url;
    if (req.headers['x-api-key'] !== chave) return 'x-api-key ausente ou errada';
    if (req.headers['anthropic-version'] !== '2023-06-01') return 'anthropic-version: ' + req.headers['anthropic-version'];
    if (!/application\/json/.test(req.headers['content-type'] || '')) return 'content-type: ' + req.headers['content-type'];
    let b;
    try { b = JSON.parse(txt); } catch (e) { return 'corpo não é JSON'; }
    if (b.model !== 'claude-sonnet-5-5') return 'modelo ' + b.model;
    if (!Number.isInteger(b.max_tokens) || b.max_tokens < 1 || b.max_tokens > 128000) return 'max_tokens ' + b.max_tokens;
    if (Array.isArray(b.system)) { if (!b.system.length || b.system.some((x) => x.type !== 'text' || !x.text)) return 'system em blocos inválido'; }
    else if (typeof b.system !== 'string' || !b.system) return 'system ausente';
    // cache_control: só {type:"ephemeral"[, ttl]}, no máximo 4 pontos, nunca em bloco de thinking
    let marcas = 0;
    const conferirMarca = (x, onde) => { if (x && x.cache_control) { marcas++; if (x.cache_control.type !== 'ephemeral') return 'cache_control.type inválido em ' + onde; if (x.type === 'thinking' || x.type === 'redacted_thinking') return 'cache_control em bloco de thinking (' + onde + ')'; } return null; };
    if (Array.isArray(b.system)) for (const x of b.system) { const e = conferirMarca(x, 'system'); if (e) return e; }
    for (const t of b.tools) { const e = conferirMarca(t, 'tools'); if (e) return e; }
    for (const m of b.messages || []) if (Array.isArray(m.content)) for (const x of m.content) { const e = conferirMarca(x, 'messages'); if (e) return e; }
    if (marcas > 4) return marcas + ' pontos de cache (máximo 4)';
    if (b.thinking && b.thinking.type === 'disabled') return 'thinking desativado (o Sonnet 5.5 recusa)';
    if (!b.tool_choice || !['auto', 'none'].includes(b.tool_choice.type)) return 'tool_choice ' + JSON.stringify(b.tool_choice) + ' (forçado é recusado no Sonnet 5.5)';
    if (!b.output_config || !ESFORCOS.includes(b.output_config.effort)) return 'output_config.effort ' + JSON.stringify(b.output_config);
    if (!Array.isArray(b.tools)) return 'tools ausente';
    const nomes = new Set();
    for (const t of b.tools) {
      if (!/^[a-zA-Z0-9_-]{1,64}$/.test(t.name || '') || nomes.has(t.name)) return 'nome de ferramenta inválido ou repetido: ' + t.name;
      nomes.add(t.name);
      if (!t.description || !t.input_schema || t.input_schema.type !== 'object') return 'esquema da ferramenta ' + t.name;
    }
    const ms = b.messages;
    if (!Array.isArray(ms) || !ms.length || ms[0].role !== 'user' || ms[ms.length - 1].role !== 'user') return 'mensagens devem começar e terminar em user';
    for (let i = 0; i < ms.length; i++) {
      const m = ms[i];
      if (i > 0 && m.role === ms[i - 1].role) return 'papéis não alternam na mensagem ' + i;
      if (m.role === 'assistant') {
        if (!Array.isArray(m.content)) return 'assistant ' + i + ' sem blocos';
        const th = m.content.find((x) => x.type === 'thinking');
        if (!th || !vistos.has(th.signature)) return 'assistant ' + i + ': bloco de thinking ausente ou assinatura desconhecida';
        if (JSON.stringify(m.content) !== vistos.get(th.signature)) return 'assistant ' + i + ': conteúdo devolvido alterado (thinking precisa voltar inalterado)';
        const usos = m.content.filter((x) => x.type === 'tool_use').map((x) => x.id);
        if (usos.length) {
          const prox = ms[i + 1];
          if (!prox || prox.role !== 'user' || !Array.isArray(prox.content)) return 'tool_use sem tool_result em seguida (mensagem ' + i + ')';
          const nRes = prox.content.findIndex((x) => x.type !== 'tool_result');
          const resultados = nRes < 0 ? prox.content : prox.content.slice(0, nRes);
          if (prox.content.slice(resultados.length).some((x) => x.type === 'tool_result')) return 'tool_result depois de texto (mensagem ' + (i + 1) + ')';
          const ids = resultados.map((x) => x.tool_use_id);
          if (ids.length !== usos.length || usos.some((u) => !ids.includes(u))) return 'tool_result não casa com tool_use (mensagem ' + (i + 1) + ')';
          if (resultados.some((x) => typeof x.content !== 'string')) return 'tool_result.content não é texto';
        }
      } else if (Array.isArray(m.content) && m.content.some((x) => x.type === 'tool_result')) {
        const ant = ms[i - 1];
        if (!ant || ant.role !== 'assistant' || !ant.content.some((x) => x.type === 'tool_use')) return 'tool_result sem tool_use antes (mensagem ' + i + ')';
      }
    }
    return null;
  }
  async function simularApi() {
    CFG.MODO = 'simulado'; // este processo só serve a API falsa; o filho faz o papel do serviço real
    if (!CFG.SIMULAR_ALUCINACAO) CFG.SIMULAR_ALUCINACAO = 'euclid';
    LEDGER.limpar(); LIT.carregar();
    console.log('PARALLAX · Células ' + VERSAO + ' — ciclo contra uma Messages API falsa (processo filho em modo real)');
    const chave = 'sk-ant-falsa-' + crypto.randomBytes(6).toString('hex');
    const vistos = new Map(), erros = [], pendentes = new Set();
    const INJETAR = { 5: 429, 12: 529, 40: 429 };
    let n = 0, reenviados = 0, comCache = 0, n401 = 0, depois401 = 0;
    const injetados = [];
    let modoFalha = ''; // '401' = a partir da 30ª requisição responde 401 (chave revogada)
    const srv = http.createServer((req, res) => {
      const partes = [];
      req.on('data', (x) => partes.push(x));
      req.on('end', () => {
        const txt = Buffer.concat(partes).toString('utf8');
        n++;
        const resp = (st, obj, cab = {}) => { res.writeHead(st, { 'content-type': 'application/json', ...cab }); res.end(JSON.stringify(obj)); };
        const prob = validarRequisicao(req, txt, chave, vistos);
        if (prob) { erros.push('#' + n + ': ' + prob); return resp(400, { type: 'error', error: { type: 'invalid_request_error', message: prob } }); }
        if (modoFalha === '401' && n >= 30) { n401++; if (n401 > 1) depois401++; return resp(401, { type: 'error', error: { type: 'authentication_error', message: 'invalid x-api-key' } }); }
        const h = U.sha256(txt);
        if (pendentes.has(h)) { pendentes.delete(h); reenviados++; }
        if (INJETAR[n] && !modoFalha) {
          const st = INJETAR[n];
          injetados.push(st);
          pendentes.add(h);
          return resp(st, { type: 'error', error: { type: st === 429 ? 'rate_limit_error' : 'overloaded_error', message: 'injetado pelo teste' } }, st === 429 ? { 'retry-after': '1' } : {});
        }
        try {
          const corpoReq = JSON.parse(txt);
          const r = SIM.responder(corpoReq);
          if (Array.isArray(corpoReq.system) && corpoReq.system.some((x) => x.cache_control)) { comCache++; r.usage.cache_creation_input_tokens = 400; r.usage.cache_read_input_tokens = Math.max(0, r.usage.input_tokens - 400); r.usage.input_tokens = 0; }
          r.content = r.content.map((x) => (x.type === 'thinking' ? { ...x, signature: 'sig-' + r.id } : x));
          vistos.set('sig-' + r.id, JSON.stringify(r.content));
          return resp(200, r);
        } catch (e) { erros.push('#' + n + ': simulador falhou: ' + e.message); return resp(500, { type: 'error', error: { type: 'api_error', message: e.message } }); }
      });
    });
    await new Promise((r) => srv.listen(0, '127.0.0.1', r));
    const porta = srv.address().port;
    const { spawn } = require('node:child_process');
    const env = { ...process.env, ANTHROPIC_API_KEY: chave, ANTHROPIC_URL: 'http://127.0.0.1:' + porta + '/v1/messages', MODO: '', RPM: '0', CONCORRENCIA: '8', MCMC_PASSOS: '2000', SIMULAR_ALUCINACAO: '', DATA_DIR: CFG.DATA_DIR, NODE_ENV: '', RENDER: '', MODELO: '' };
    const t0 = Date.now();
    const filho = spawn(process.execPath, [process.argv[1], '--simular-api-filho'], { env, stdio: ['ignore', 'pipe', 'inherit'] });
    let saida = '';
    filho.stdout.on('data', (x) => { saida += x; });
    const codigo = await new Promise((r) => filho.on('close', r));
    const linha = saida.split('\n').find((l) => l.startsWith('RESULTADO '));
    const n1 = n, injetados1 = injetados.length, reenviados1 = reenviados, comCache1 = comCache;
    // segunda rodada: chave revogada no meio do ciclo (401 a partir da 30ª requisição); sem 429/529 injetados
    modoFalha = '401'; n = 0; vistos.clear();
    const filho2 = spawn(process.execPath, [process.argv[1], '--simular-api-filho'], { env, stdio: ['ignore', 'pipe', 'inherit'] });
    let saida2 = '';
    filho2.stdout.on('data', (x) => { saida2 += x; });
    const codigo2 = await new Promise((r) => filho2.on('close', r));
    const linha2 = saida2.split('\n').find((l) => l.startsWith('RESULTADO '));
    // terceira rodada: teto de gasto baixo (US$ 0,30 com os tokens falsos) → o ciclo degrada, não derruba
    modoFalha = ''; n = 0; vistos.clear();
    const filho3 = spawn(process.execPath, [process.argv[1], '--simular-api-filho'], { env: { ...env, TETO_USD: '0.3' }, stdio: ['ignore', 'pipe', 'inherit'] });
    let saida3 = '';
    filho3.stdout.on('data', (x) => { saida3 += x; });
    const codigo3 = await new Promise((r) => filho3.on('close', r));
    const linha3 = saida3.split('\n').find((l) => l.startsWith('RESULTADO '));
    srv.close();
    const casos = [];
    const k = (nome, f) => casos.push([nome, f]);
    let R = null;
    k('processo filho terminou e devolveu o resultado', () => { afirmar(codigo === 0 && linha, 'código ' + codigo + ', saída: ' + saida.slice(-500)); R = JSON.parse(linha.slice(10)); });
    k('ciclo concluído em modo real contra a API falsa', () => afirmar(R && R.modo === 'real' && R.estado === 'concluido', JSON.stringify(R && { modo: R.modo, estado: R.estado, erro: R.erro })));
    k('nenhuma requisição inválida (cabeçalhos, tool_choice, effort, alternância, pares tool_use/tool_result, thinking devolvido inalterado)', () => { afirmar(!erros.length, erros.slice(0, 5).join(' | ')); return (n1 + n) + ' requisições conferidas nas três rodadas'; });
    k('429 e 529 injetados foram reenviados com a mesma requisição', () => {
      afirmar(injetados1 === 3 && reenviados1 === 3, 'injetados ' + injetados1 + ', reenviados ' + reenviados1);
      afirmar(R.custo.reenvios >= 3 && R.custo.erros >= 3, JSON.stringify(R.custo));
    });
    k('nenhum papel em erro', () => { const e = Object.entries(R.celulas).flatMap(([id, s]) => s.erros.map((x) => id + '.' + x)); afirmar(!e.length, e.join('; ')); });
    k('célula euclid suprimida pela guarda (alucinação injetada pela API falsa)', () => afirmar(R.celulas.euclid.suprimida && R.celulas.euclid.violacoes >= 1, JSON.stringify(R.celulas.euclid)));
    k('custo contabilizado pelo campo usage das respostas', () => { afirmar(R.custo.usd > 0 && R.custo.chamadas > 0, JSON.stringify(R.custo)); return R.custo.chamadas + ' chamadas, US$ ' + U.fmt(R.custo.usd, 2) + ' com os preços configurados (tokens falsos do simulador)'; });
    k('publicações e barramento', () => { afirmar(R.publicados > 0 && R.contador.rejeitados === 0, JSON.stringify({ publicados: R.publicados, contador: R.contador })); return R.publicados + ' medidas, ' + R.contador.quadros + ' quadros URB1'; });
    k('cache de prompt: system em blocos com marca, custo separa escrita e leitura', () => {
      afirmar(comCache1 > 0, 'nenhuma requisição com marca de cache');
      afirmar(R.custo.cache_escrita > 0 && R.custo.cache_leitura > 0, JSON.stringify(R.custo));
      const esperado = ((R.custo.entrada + R.custo.cache_escrita * CFG.FATOR_CACHE_ESCRITA + R.custo.cache_leitura * CFG.FATOR_CACHE_LEITURA) / 1e6) * CFG.PRECO_ENTRADA + (R.custo.saida / 1e6) * CFG.PRECO_SAIDA;
      afirmar(Math.abs(R.custo.usd - esperado) < 1e-9, 'usd ' + R.custo.usd + ' ≠ ' + esperado);
      return comCache1 + ' requisições com cache';
    });
    let R2 = null;
    k('chave revogada no meio do ciclo (401): ciclo termina em erro, nenhuma célula fica ativa', () => {
      afirmar(codigo2 === 0 && linha2, 'código ' + codigo2 + ', saída: ' + saida2.slice(-400));
      R2 = JSON.parse(linha2.slice(10));
      afirmar(R2.estado === 'erro' && /chave da API recusada/.test(R2.erro || ''), JSON.stringify({ estado: R2.estado, erro: R2.erro }));
      afirmar(R2.ativos === 0, R2.ativos + ' papel(is) ainda ativo(s) depois do fim');
      afirmar(R2.eventosDepoisDoFim === 0, R2.eventosDepoisDoFim + ' evento(s) depois do fim');
    });
    k('depois do 401 as outras células param de chamar a API (só o que já estava em voo)', () => { afirmar(depois401 <= 8, depois401 + ' requisições depois do 401'); return depois401 + ' requisição(ões) em voo'; });
    k('teto de gasto atingido no meio: ciclo termina "concluido" com aviso, redução feita, parecer não redigido, custo parou perto do teto', () => {
      afirmar(codigo3 === 0 && linha3, 'código ' + codigo3 + ', saída: ' + saida3.slice(-400));
      const R3 = JSON.parse(linha3.slice(10));
      afirmar(R3.estado === 'concluido' && R3.tetoAtingido && /teto de gasto/.test(R3.aviso || ''), JSON.stringify({ estado: R3.estado, teto: R3.tetoAtingido, aviso: R3.aviso, erro: R3.erro }));
      afirmar(R3.rede.estado === 'aguardando', 'parecer deveria ter sido pulado: ' + R3.rede.estado);
      afirmar(R3.reducao > 0, 'redução sem registros');
      afirmar(R3.custo.usd >= 0.3 && R3.custo.usd < 0.3 + 0.1, 'custo final ' + R3.custo.usd + ' (chamadas em voo podem passar um pouco do teto)');
      afirmar(R3.ativos === 0 && R3.eventosDepoisDoFim === 0, 'papéis ativos ' + R3.ativos + ', eventos depois do fim ' + R3.eventosDepoisDoFim);
      return 'teto na fase "' + R3.tetoAtingido + '", custo final US$ ' + U.fmt(R3.custo.usd, 2) + ', ' + R3.reducao + ' registros da redução';
    });
    const r = await rodar(casos);
    console.log('\n  duração ' + Math.round((Date.now() - t0) / 1000) + ' s');
    console.log(r.nOk + ' de ' + casos.length + ' verificações passaram' + (r.falhas.length ? '; FALHARAM: ' + r.falhas.join(' · ') : ''));
    return !r.falhas.length;
  }
  async function filhoApi() {
    LEDGER.limpar(); LIT.carregar();
    let terminou = false, eventosDepoisDoFim = 0;
    const fim = new Promise((res) => ORQ.ouvintes.add((ev) => {
      if (terminou && ev.t !== 'custo') eventosDepoisDoFim++;
      if (ev.t === 'ciclo' && ev.fase) process.stderr.write('  [filho] fase ' + ev.fase + '\n');
      if (ev.t === 'log' && /^SUPRIMIDO|reenvi|falhou|^ciclo/.test(ev.msg)) process.stderr.write('  [filho] ' + ev.msg + '\n');
      if (ev.t === 'fim') { terminou = true; res(ev); }
    }));
    await ORQ.executar({ celulas: ['desi', 'euclid', 'rubin', 'des', 'hubble', 'planck', 'act', 'spt'], rodadas: 2, objetivo: 'Teste de protocolo contra a API falsa.' });
    await fim;
    await U.dormir(1500); // janela para pegar evento atrasado de alguma célula que continuasse rodando
    const c = ORQ.atual();
    const ativos = Object.values(c.celulas).flatMap((s) => Object.values(s.papeis)).filter((v) => v.estado === 'ativo').length;
    const out = {
      modo: CFG.MODO, estado: c.estado, erro: c.erro || null, aviso: c.aviso || null, tetoAtingido: c.tetoAtingido || null, custo: { ...IA.custo }, contador: c.contador, publicados: LEDGER.listar({ publicado: true }).length, ativos, eventosDepoisDoFim,
      reducao: LEDGER.listar({ celula: 'rede' }).length,
      celulas: Object.fromEntries(Object.entries(c.celulas).map(([id, s]) => [id, { peso: s.peso, suprimida: s.suprimida, violacoes: s.violacoes, erros: Object.entries(s.papeis).filter(([, v]) => v.estado === 'erro').map(([p, v]) => p + ': ' + v.erro) }])),
      rede: { estado: c.rede.estado, erro: c.rede.erro || null },
    };
    process.stdout.write('RESULTADO ' + JSON.stringify(out) + '\n');
  }
  return { offline, validar, simular, simularApi, filhoApi, lerCadeia, resumoPonderado, validarRequisicao };
})();

/* ── 20 · entrada ──────────────────────────────────────────────────────── */
(async () => {
  const modo = process.argv[2] || '';
  const sair = (ok) => process.exit(ok ? 0 : 1);
  try {
    if (modo === '') SERV.iniciar();
    else if (modo === '--teste') sair(await TESTES.offline());
    else if (modo === '--validar') sair(await TESTES.validar());
    else if (modo === '--simular') sair(await TESTES.simular());
    else if (modo === '--simular-api') sair(await TESTES.simularApi());
    else if (modo === '--simular-api-filho') { await TESTES.filhoApi(); process.exit(0); }
    else if (modo === '--versao') { console.log(VERSAO); process.exit(0); }
    else { console.error('uso: node server.js [--teste | --validar | --simular | --simular-api | --versao]'); process.exit(2); }
  } catch (e) {
    console.error('[parallax] falha:', e);
    process.exit(1);
  }
})();

