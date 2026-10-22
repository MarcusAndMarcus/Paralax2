# PARALLAX · Células

Web service de cosmologia focado em matéria escura e energia escura. Tem 17 células, uma por telescópio. Cada célula roda 8 instâncias do Claude Sonnet, uma por papel:

- COLETOR
- ANALISTA
- TEÓRICO
- CÉTICO
- VERIFICADOR
- AUDITOR
- EMBAIXADOR
- RELATOR

As células trocam digests por gossip no barramento URB1, no mesmo quadro do UROBOROS. Um Polo V determinístico faz a redução da rede. Nenhum número nasce de modelo de linguagem: todo número vem de arquivo oficial, consulta TAP, literatura conferida ou cálculo feito aqui, e fica num registro com proveniência.

O serviço é um arquivo único (`server.js`) para Node 22, sem dependências.

## Deploy manual (GitHub → Render)

### 1. GitHub

1. Entre no GitHub e crie um repositório novo (**New repository**): nome `parallax-celulas`, **Private**, sem README, sem .gitignore, sem licença.
2. Na página do repositório vazio, toque em **uploading an existing file** (ou **Add file → Upload files**).
3. Envie os 4 arquivos para a **raiz**, sem pasta: `server.js`, `package.json`, `README.md`, `.gitignore`. O `render.yaml` é opcional: só serve se você criar o serviço por Blueprint; na criação manual ele é ignorado.
4. Toque em **Commit changes**.

### 2. Render

1. Entre no Render e toque em **New → Web Service**.
2. Em **Source Code**, escolha **Git Provider**, conecte o GitHub se ainda não estiver conectado e selecione `parallax-celulas`. Se o repositório não aparecer, toque em **Configure account** e dê acesso a ele.
3. Preencha:

| campo | valor |
|---|---|
| Name | `parallax-celulas` |
| Region | a mais próxima (`Virginia` ou `Ohio` para o Brasil) |
| Branch | `main` |
| Language | `Node` |
| Build Command | `npm install` |
| Start Command | `node server.js` |
| Instance type / plano | 1 CPU · 2 GB (`1c-2g`). O plano Free tem 512 MB e dorme com inatividade; não foi testado. |

4. Em **Environment Variables**, adicione:

| chave | valor |
|---|---|
| `ANTHROPIC_API_KEY` | sua chave da API |
| `ACCESS_TOKEN` | uma senha longa que você inventa; é a senha do painel |
| `DATA_DIR` | `/var/data` (só se anexar o disco do passo 5; sem disco, não defina) |

5. Em **Advanced**:
   - **Health Check Path**: `/saude`
   - **Disk** (opcional, recomendado): Name `dados`, Mount Path `/var/data`, Size 5 GB. Sem disco, o Render apaga cache de dados e dossiês a cada deploy ou reinício.
6. Toque em **Create Web Service** e espere o deploy ficar **Live**. O endereço do serviço aparece no topo e termina em `.onrender.com`.

### 3. Primeiro uso

1. Abra o endereço do serviço e digite o `ACCESS_TOKEN` no campo do painel. O navegador guarda.
2. Toque em **1 · Testar conexões**. A linha da Anthropic precisa dar `ok`; as linhas de TAP e produtos mostram o que está acessível a partir do Render.
3. Toque em **2 · Ciclo de teste** (DESI, DES, Hubble e Planck, 1 rodada). Espere `concluido`. O custo acumulado aparece no topo.
4. Toque em **3 · Ciclo completo**.

### Atualizar uma versão nova

No GitHub, abra o repositório, toque em **Add file → Upload files**, envie o `server.js` novo (e o `README.md`) e confirme. O Render refaz o deploy sozinho.

## Arquivos

| arquivo | o que é |
|---|---|
| `server.js` | o serviço inteiro: painel, API, SSE, leitores de dados, pipelines, Polo V, orquestração e testes |
| `package.json` | sem dependências; `npm start` roda `node server.js`; Node fixado em `22.x` |
| `README.md` | este texto |
| `.gitignore` | ignora `dados/` (cache e dossiês locais) |
| `render.yaml` | opcional: Blueprint com os mesmos campos acima, para quem preferir **New → Blueprint** |

## Detalhes do serviço no Render

Sem `ANTHROPIC_API_KEY`, o serviço sobe em modo simulado e não chama a API. Em produção (o Render define `RENDER`), sem `ACCESS_TOKEN` as rotas que gastam API respondem 503: ciclo, parar e testar conexões. A leitura (painel, estado, registros, dossiês) é pública para quem tiver a URL.

**Deploy ou reinício durante um ciclo.** O Render manda SIGTERM à instância antiga; o serviço interrompe o ciclo, salva o dossiê com estado `interrompido` e encerra. O ciclo não continua na instância nova: inicie outro.

**Disco.** Com o disco em `/var/data` e `DATA_DIR=/var/data`, dossiês e cache de dados sobrevivem a deploys. Com disco anexado, o Render desliga o deploy sem interrupção e não deixa escalar para mais de uma instância.

**Memória.**

- Medido aqui, não no Render: pico de RSS num ciclo simulado completo das 17 células, com os três pipelines e o MCMC da redução:
  - 332 MB com o cache vazio, baixando os dados do GitHub;
  - 273 MB com o cache já preenchido.
- Por isso a instância de 2 GB. Uma de 512 MB fica justa, e o plano Free (512 MB) ainda dorme com inatividade.
- Arquivos baixados do índice do eROSITA ficam limitados a 60 MB e às primeiras 50 000 linhas, e não ficam retidos em memória; respostas TAP, a 30 MB.

**Versão do Node.** O `package.json` fixa `22.x`. O Render usa a versão 24 por padrão em serviços novos, e ela não foi testada aqui. O autoteste rodou em Node 22.22.0 e 20.20.0; a validação e as simulações, em 22.22.0.

**Cache de prompt.** O system prompt e o último bloco de cada mensagem do usuário levam `cache_control` (2 dos 4 pontos permitidos). No Sonnet 5.5 a leitura do cache custa 0,05× o preço de entrada e a escrita 1,25×; a conta de custo do painel usa esses fatores sobre os campos `usage` da API. A economia real não foi medida. `CACHE_PROMPT=0` desliga.

## Variáveis de ambiente

| variável | padrão | efeito |
|---|---|---|
| `ANTHROPIC_API_KEY` | — | chave da API; ausente = modo simulado |
| `ACCESS_TOKEN` | — | exigido para ciclo, parar e testar sempre que houver chave real, em produção, ou quando definido |
| `MODELO` | `claude-sonnet-5-5` | modelo dos 8 papéis |
| `ESFORCO` | `medium` | `output_config.effort` dos papéis comuns |
| `ESFORCO_CRITICO` | `high` | esforço do CÉTICO, do RELATOR e do redator do parecer |
| `MAX_TOKENS` | `16000` | teto de saída por chamada |
| `CONCORRENCIA` | `6` | chamadas simultâneas à API |
| `RPM` | `40` | chamadas por minuto (0 = sem limite); suba conforme o tier da conta |
| `TETO_USD` | `150` | teto de gasto **por ciclo** (estimado pelos campos `usage` da API). Ao atingir, as chamadas à API param; a redução determinística ainda roda e o ciclo termina `concluido` com aviso, sem parecer |
| `PRECO_ENTRADA_USD_MTOK` / `PRECO_SAIDA_USD_MTOK` | `2` / `10` | preço do Sonnet 5.5 usado na conta de custo |
| `FATOR_CACHE_ESCRITA` / `FATOR_CACHE_LEITURA` | `1.25` / `0.05` | multiplicadores do cache de prompt sobre o preço de entrada |
| `CACHE_PROMPT` | `1` | `0` desliga as marcas de cache nas requisições |
| `RODADAS_MAX` | `3` | rodadas de gossip (o painel pode mudar por ciclo, até 6) |
| `LIMIAR_PESO` | `0.5` | célula com peso abaixo disso não fala no gossip |
| `LIMIAR_TENSAO` | `3` | tensão (σ) que o Polo V registra entre medidas independentes |
| `PASSOS_FERRAMENTA` | `8` | rodadas de ferramenta por papel antes da resposta forçada |
| `MCMC_PASSOS` | `20000` | passos de MCMC; com supernovas o limite é 8000 |
| `TIMEOUT_API_MS` / `TIMEOUT_DADOS_MS` | `600000` / `180000` | tempos-limite da API e dos downloads |
| `DATA_DIR` | `./dados` | cache de arquivos e dossiês |

## Uso

O painel fica em `/` e tem sete partes:

- **00 ciclo**: pergunta, escolha de células, rodadas de gossip, parecer, senha e os botões 1 · Testar conexões, 2 · Ciclo de teste e 3 · Ciclo completo. A senha fica guardada neste aparelho.
- **01 células**: os 17 cartões com os 8 papéis, o peso e o digest. Clique num cartão para ver o texto de cada papel e o resultado da guarda.
- **02 barramento URB1**
- **03 Polo V**
- **04 rede**: medidas publicadas e tensões.
- **05 parecer**
- **06 conexões**: resultado de **Testar conexões**.

As rotas estão abaixo. Leitura é pública. Ciclo, parar e testar exigem `Authorization: Bearer <ACCESS_TOKEN>`.

```
GET  /saude
GET  /api/estado            estado do ciclo atual (inclui seq para o SSE)
GET  /api/eventos?desde=N   SSE com retomada (Last-Event-ID)
GET  /api/ledger?celula=&grandeza=&publicado=1
GET  /api/dossies           GET /api/dossies/<id>
GET  /api/celula/<id>       textos, guarda e execuções de uma célula
POST /api/ciclo             {"objetivo": "...", "celulas": ["desi","des"], "rodadas": 3, "parecer": true}
POST /api/parar
GET  /api/testar            API (GET /v1/models e um POST /v1/messages mínimo), 7 serviços TAP, 8 produtos de dados
```

Exemplo pelo Termux:

```
curl -X POST https://SEU-SERVICO.onrender.com/api/ciclo \
  -H "authorization: Bearer $ACCESS_TOKEN" -H "content-type: application/json" \
  -d '{"celulas":["desi","des","hubble","planck","act","spt"],"rodadas":2}'
```

## O que acontece num ciclo

**Fase 01, local.** Cada célula executa, com todas as células em paralelo:

COLETOR → ANALISTA → (TEÓRICO ∥ CÉTICO) → VERIFICADOR → AUDITOR → RELATOR

Cada papel usa só as ferramentas do seu papel.

**Peso de influência.** O peso de cada célula é calculado assim:

> peso = (papéis concluídos / 7) × f(gravidade) × (0,5 se o AUDITOR reprovar)

A gravidade soma as violações da guarda: uma medida inventada (valor ± incerteza sem registro) vale 1; um número solto sem registro ou um id inexistente vale 0,5. f vale 1 para gravidade 0; 0,7 até 0,5; 0,45 abaixo de 2; 0,2 abaixo de 3; 0,1 daí em diante. Assim um único número solto reduz o peso sem suprimir a célula; uma medida inventada suprime. Célula com peso abaixo de `LIMIAR_PESO` fica suprimida: o EMBAIXADOR dela não roda, o digest dela não chega às vizinhas, e o log registra `SUPRIMIDO <célula> → <vizinha>`.

**Fase 02, gossip.** O EMBAIXADOR de cada célula ativa recebe os digests das vizinhas em quadros URB1. As vizinhas são duas no anel mais até três que publicaram a mesma grandeza. O gossip para quando a mudança média dos digests, 1 − Jaccard, fica abaixo de 0,12.

**Fase 03, veredito.** O RELATOR de cada célula reescreve o veredito com o que a rede confirmou ou contradisse.

**Fase 04, redução (Polo V).** Tudo aqui é determinístico:

- identidades;
- ajuste conjunto DESI DR2 BAO + DES-Dovekie em ΛCDM e w0waCDM;
- r_d tardio = h·r_d ÷ h;
- r_d pelo CMB, com Aubourg 2015;
- H₀ pela régua inversa;
- tensões e combinações.

O Polo V recusa combinar medidas que compartilham dados (`dados_usados`).

**Fase 05, parecer.** Um Sonnet redige o parecer da rede sob a mesma regra de ouro e sem usar o texto das células suprimidas.

**Dossiê.** Tudo vai para `DATA_DIR/dossies/<id>.json`: textos, guarda, registro com SHA-256 dos arquivos e tabela da rede.

## Regras contra número inventado

- **Registro.** Todo valor tem etiqueta: [D] dado (arquivo, TAP, literatura), [C] calculado aqui, [F] fato com fonte, [L] lacuna. `publicar_medida` recebe só o id; o valor vem do registro.
- **Guarda.** Depois de cada papel, todo número do texto precisa existir no registro visível para a célula ou ter sido devolvido por uma ferramenta na célula.
- **Pares.** Um par `valor ± σ` ou `valor +σ₊/−σ₋` precisa vir inteiro de um **mesmo** registro.
- **Isenções.** Ficam isentos só inteiros até 20, anos de 1990 a 2040 e níveis de confiança (68%, 95%, 99%, 99,7%). Dígitos colados a letras não contam como número ("100θ*", "3x2pt", "DR2", "C01"); σ e % podem vir colados ("5,4σ").
- **Constantes.** As constantes do motor (c, T_CMB, N_eff, Σmν = 0,06 eV, fórmula de Aubourg 2015) estão num fato visível a todas as células, então citá-las não conta como invenção.
- **Ids.** Id citado que não existe conta como violação. Só a forma real dos ids é reconhecida (`C-` ou `D-lit-` + 8 hex, `F-<célula>-<n>`), em qualquer caixa; "F-score" ou "C-type" não contam.
- **Hashes.** Um prefixo de SHA-256 só com dígitos, precedido de "SHA-256" ou "hash", não conta como número.
- **TAP.** O texto ADQL escrito pelo modelo não entra na guarda.
- **Recálculo.** O VERIFICADOR refaz cada [C] com passo de integração menor e outro ponto de partida. Aprova se a diferença for menor que 0,1σ num ajuste determinístico, ou 0,3σ num MCMC, que tem ruído de Monte Carlo (medido abaixo).

## As 17 células

| célula | situação | o que calcula ou usa |
|---|---|---|
| DESI | validado | DESI DR2 BAO (13 medidas + covariância) → Ω_m, h·r_d, w, w₀, w_a |
| Euclid | parcial | TAP IRSA (Q1); sem pipeline cosmológico |
| Rubin | parcial | fatos (LSST iniciado, dados de imagem com período proprietário) |
| Blanco/DECam (DES) | validado | DES-Dovekie, 1820 SNe Ia com covariância inversa → Ω_m, w…; literatura Y6 3x2pt |
| Subaru (HSC) | literatura | S₈ do HSC Y3 |
| JWST | literatura | fatos |
| Hubble | validado | Pantheon+SH0ES (Cefeidas calibram M) → H₀, Ω_m, M |
| Gaia | ingestão | TAP Gaia e SIMBAD |
| Planck | literatura | resumo do Planck 2018 VI (H₀, Ω_m, σ₈, ω_b, ω_c, 100θ*) |
| ACT | literatura | ACT DR6 com DESI |
| SPT | literatura | SPT-3G D1 sozinho |
| CHIME | literatura | fatos (21 cm, FRBs) |
| MeerKAT | literatura | fatos (MeerKLASS) |
| ASKAP | ingestão | TAP CASDA |
| eROSITA | ingestão | TAP VizieR e diretório de catálogos do DR1; literatura eRASS1 |
| Chandra | ingestão | TAP HEASARC |
| Fermi-LAT | ingestão | TAP HEASARC |

Os arquivos de dados estão fixados nos commits conferidos em 16/out/2026:

- CobayaSampler/bao_data `bb0c1c9`
- des-science/DES-SN5YR `c9a4fca`
- PantheonPlusSH0ES/DataRelease `c447f0f`

## Testes rodados antes da entrega

`node server.js --teste` (offline): **32 de 32** passaram, em Node 22.22.0 e 20.20.0. Cobrem:

- CRC16 e URB1;
- álgebra e MCMC;
- cosmologia contra soluções analíticas e contra o r_drag publicado pelo Planck 2018;
- leitores NPY, NPZ, FITS, CSV e VOTable;
- guarda, ledger e Polo V;
- laço de agente;
- painel;
- rotas HTTP, token, teto de 50 conexões de eventos e chaves herdadas de objeto;
- literatura atribuída a duas células (cópia por célula na rede);
- limite de linhas em FITS e CSV;
- um ciclo de 4 células;
- parar no meio do ciclo (estado `interrompido`, nenhum papel fica ativo, dossiê salvo).

`node server.js --validar` baixa os dados e as cadeias oficiais, calcula os alvos na hora e compara. Resultado: **10 de 10 comparações dentro do critério**, que é |Δ| < 0,2σ e razão de σ entre 0,8 e 1,2 (Laplace), ou |Δ| < 0,25σ e razão entre 0,75 e 1,25 (MCMC).

| ajuste | parâmetro | cadeia oficial | aqui | Δ |
|---|---|---|---|---|
| DESI DR2 BAO, ΛCDM | Ω_m | 0,2973 ± 0,0086 | 0,2972 ± 0,0086 | 0,022σ |
| DES-Dovekie, ΛCDM | Ω_m | 0,3306 ± 0,0154 | 0,3302 ± 0,0153 | 0,028σ |
| Pantheon+SH0ES, ΛCDM | Ω_m | 0,3328 ± 0,0181 | 0,3317 ± 0,0180 | 0,061σ |
| | H₀ | 73,54 ± 1,02 | 73,53 ± 1,02 | 0,011σ |
| | M | −19,2436 ± 0,0296 | −19,2441 | 0,016σ |
| BAO + Dovekie, wCDM (MCMC) | Ω_m | 0,2972 ± 0,0084 | 0,2969 +0,0081/−0,0082 | 0,032σ |
| | w | −0,9087 +0,0348/−0,0369 | −0,9065 +0,0374/−0,0405 | 0,059σ |
| BAO + Dovekie, w0waCDM (MCMC) | Ω_m | 0,3132 +0,0130/−0,0156 | 0,3132 +0,0144/−0,0163 | 0,003σ |
| | w₀ | −0,8425 +0,0719/−0,0649 | −0,8423 +0,0711/−0,0641 | 0,004σ |
| | w_a | −0,5255 +0,4399/−0,4488 | −0,5166 +0,4511/−0,5002 | 0,020σ |

`node server.js --simular`: **14 de 14**. É o ciclo completo das 17 células com o simulador no lugar do Claude; ferramentas, dados, pipelines, URB1, gossip, peso e Polo V são o código real. Uma célula (Euclid) foi programada para inventar "Ω_m = 0,412 ± 0,003". Resultado:

- a guarda pegou o número inventado;
- o peso do Euclid caiu para 0,45 e a célula foi suprimida do gossip;
- não houve violação nas outras 16 células;
- 113 quadros URB1, nenhum rejeitado;
- duração de 51 s com cache e 67 s com cache vazio.

`node server.js --simular-api`: **12 de 12**. São três ciclos de 8 células num processo em modo real contra uma Messages API falsa, que conferiu 300 requisições:

- cabeçalhos, `tool_choice`, `effort` e alternância de papéis;
- pares `tool_use`/`tool_result`;
- blocos de thinking devolvidos sem alteração;
- marcas de cache: só `ephemeral`, no máximo 4, nunca em bloco de thinking; custo separa escrita e leitura;
- injeção de dois 429 e um 529, todos reenviados;
- chave revogada no meio do ciclo (401 a partir da 30ª requisição): o ciclo termina em `erro` com a causa, nenhuma célula continua gastando API e nenhum evento sai depois do fim;
- teto de gasto atingido no meio (US$ 0,30 com os tokens falsos): o ciclo termina `concluido` com aviso, a redução roda, o parecer é pulado e o custo final ficou em US$ 0,33.

**Revisão independente.** Um segundo agente, sem ter visto o código ser escrito, revisou o `server.js` inteiro. Os achados de severidade alta e média foram corrigidos nesta versão: arquivo de índice grande materializado em memória; falso positivo da guarda com prefixo de hash só de dígitos; uma violação qualquer suprimia a célula; teto de gasto derrubava o ciclo inteiro; par TEÓRICO/CÉTICO fora do controle de abort; corpos HTTP sem limite e conexões de eventos sem teto; literatura de duas células com um só publicador. Ficaram sem mudança, por decisão: a leitura pública do painel e a tolerância a sinal trocado na guarda.

**Painel.** Foi aberto no Chromium:

- em 1280 px funcionaram o ciclo completo, a gaveta da célula, a retomada do SSE após recarregar e testar conexões;
- em 800 e 390 px não há rolagem horizontal;
- em todas as larguras, nenhum erro de JavaScript.

O único erro de console foi a fonte do Google, bloqueada na rede do teste.

**Ruído do MCMC.** Medido com três sementes no ajuste BAO + Dovekie: diferença máxima entre medianas de 0,126σ em w0waCDM e de 0,063σ em wCDM.

## O que não foi medido

- **Custo de um ciclo completo.** Não medido. Uma estimativa grosseira (não medida) do revisor independente: cerca de 190 execuções de agente por ciclo completo, com US$ 0,3 a 0,5 cada → US$ 50 a 100 por ciclo. Por isso o teto padrão é US$ 150 e o **Ciclo de teste** vem antes.
- **Nenhuma chamada real à API da Anthropic.** Não havia chave aqui. O formato da requisição foi conferido contra a documentação e contra a API falsa. O primeiro teste real é o botão **Testar conexões**: faz `GET /v1/models/<modelo>` e um `POST /v1/messages` mínimo. Esse POST tem teto de 1024 tokens de saída, então custa no máximo cerca de US$ 0,012.
- **Custo e duração de um ciclo real das 17 células.** Não medidos; o painel mostra o custo acumulado durante o ciclo, e `TETO_USD` corta.
- **Taxa de falso positivo da guarda com texto real do Sonnet.** Não medida. O simulador só escreve números do registro.
- **Serviços TAP e diretório do eROSITA.** Os sete serviços (Gaia, IRSA, CASDA, VizieR, SIMBAD, HEASARC, Data Lab) e o diretório do eROSITA ficaram inacessíveis da rede de teste. Os endereços de VizieR, SIMBAD e HEASARC não foram conferidos. Confira tudo com **Testar conexões** no Render.
- **CMB.** Planck, ACT e SPT entram só pela literatura conferida. Verossimilhanças de CMB não estão implementadas.
- **Células sem pipeline validado.** Só DESI, DES e Hubble têm pipeline validado. As outras 14 células trabalham com ingestão (TAP, catálogos), estatística descritiva, fatos e literatura conferida, e não produzem medida cosmológica própria.
- **Limite da guarda.** Um número isolado de baixa precisão ainda pode coincidir por acaso com algum valor do registro. A exigência de par do mesmo registro reduz isso, mas não elimina.
