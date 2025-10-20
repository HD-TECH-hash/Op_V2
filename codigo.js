// codigo.js — CRION busca de alta precisão (index local, zero alucinação)

/* =========================
   UF ↔ Estado (equivalências)
   ========================= */
const UF_MAP = {
  ac:["acre","ac"], al:["alagoas","al"], ap:["amapa","amapá","ap"], am:["amazonas","am"],
  ba:["bahia","ba"], ce:["ceara","ceará","ce"], df:["distrito federal","df","brasilia","brasília"],
  es:["espirito santo","espírito santo","es"], go:["goias","goiás","go"], ma:["maranhao","maranhão","ma"],
  mt:["mato grosso","mt"], ms:["mato grosso do sul","ms"], mg:["minas gerais","mg"],
  pa:["para","pará","pa"], pb:["paraiba","paraíba","pb"], pr:["parana","paraná","pr"],
  pe:["pernambuco","pe"], pi:["piaui","piauí","pi"], rj:["rio de janeiro","rj"],
  rn:["rio grande do norte","rn"], rs:["rio grande do sul","rs"], ro:["rondonia","rondônia","ro"],
  rr:["roraima","rr"], sc:["santa catarina","sc"], sp:["sao paulo","são paulo","sp"],
  se:["sergipe","se"], to:["tocantins","to"]
};

/* =========================
   Cidades e aliases
   ========================= */
const CITY_ALIASES = {
  "sao cristovao": [
    "sao cristovao","sao-cristovao","s cristovao","s.cristovao",
    "sao cristóvao","sao cristóvão","sao-cristóvão","s cristovão","s.cristovão",
    "são cristovao","são cristóvão"
  ],
  "sao bernardo": [
    "sao-bernardo","s bernardo","s.bernardo","sao bernado","sao bernado do campo", "bernardo", "samp"
  ],
  "sao jose dos campos": ["sjc","s jose dos campos","s.jose dos campos"],
  "belo horizonte": ["bh"],
  "rio de janeiro": ["rj capital","rio"],
  "sao paulo": ["sp capital","sampa"],
  "porto alegre": ["poa"],
  "cuiaba": ["cuiabá"],
  "goiania": ["goiânia"],
  "joao pessoa": ["joão pessoa"],
  "tres lagoas": ["três lagoas"],
  "mossoro": ["mossoró"],
  "uberlandia": ["uberlândia"],
  "ribeirao preto": ["ribeirão preto"],
  "vitoria de santo antao": ["vitória de santo antão"],
  // estados como “cidades” úteis para reforço de contexto
  "maranhao": ["ma"], "amazonas": ["am"], "sergipe": ["se"],
  "pernambuco": ["pe"], "para": ["pará","pa"],
};

/* =========================
   Tokens especiais → cidade/UF
   ========================= */
const SPECIAL_CITY_TOKENS = {
  // “samp” nos nomes da Affix/ES → força ES + São Bernardo
  "samp": { city:"sao bernardo", uf:"es" }
};

/* =========================
   Normalização e tokenização
   ========================= */
const STOP = new Set(["de","da","do","das","dos","e","a","o","as","os","the"]);

function hardCorrections(sNorm) {
  // Corrige erros comuns depois de remover acentos
  // ex: "espirio" => "espirito", "bernado" => "bernardo"
  return sNorm
    .replace(/\bespirio\b/g, "espirito")
    .replace(/\bbernado\b/g, "bernardo")
    .replace(/\bcristovao\b/g, "cristovao"); // estabiliza
}

const norm = s => {
  const base = String(s||"")
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")   // remove acentos e cedilha (ç -> c)
    .replace(/[._]/g, " ")
    .replace(/[-/]/g, " ")            // hífen e barra como espaço
    .replace(/\s+/g, " ")
    .trim();

  // Unifica “são” -> “sao” já garantido por remoção de acentos.
  // Reforço para variantes isoladas “s ao”, etc., por OCR:
  const reinforced = base
    .replace(/\bsa[o0]\b/g, "sao")         // sao / sa0
    .replace(/\bs ao\b/g, "sao");          // s ao

  return hardCorrections(reinforced);
};

const tokenize = s => norm(s)
  .replace(/[^\p{Letter}\p{Number}\s]/gu, " ")
  .split(/\s+/)
  .filter(t => t && !STOP.has(t));

/* =========================
   Marcas/domínios
   ========================= */
const BRAND_DOMAINS = { affix:"affix.com.br", alter:"alter.com.br" };
const detectBrands = qn => ({
  hasAffix: /\baffix\b/i.test(qn),
  hasAlter: /\balter\b/i.test(qn)
});

/* =========================
   Datas em nome → boost
   ========================= */
function extractMY(nameN){
  const m = nameN.match(/(^|[^0-9])(0[1-9]|1[0-2])[-_](\d{2})(?=($|[^0-9]))/);
  return m ? {year:2000 + +m[3], month:+m[2]} : null;
}
const dateScore = item => { const my = extractMY(item.nameN); return my ? my.year*12 + my.month : 0; };

/* =========================
   Helpers de match
   ========================= */
const wordsSlug = s => ` ${tokenize(s).join(" ")} `;
const containsWord = (slug, t) => slug.includes(` ${t} `);
const containsPhrase = (slug, phrase) => {
  const p = tokenize(phrase).join(" ");
  return p && slug.includes(` ${p} `);
};
const countWholeWords = (item, terms) => {
  let c=0; for(const t of terms){ if(containsWord(item.slug,t) || item.kws.has(t)) c++; } return c;
};

/* =========================
   Reforço UF forte e “ES-Manual”
   ========================= */
function hasUFStrong(raw, uf){
  const sig = uf.toUpperCase();
  const re = new RegExp(`(^|[^A-Za-z])${sig}([^A-Za-z]|$)`);
  return re.test(raw);
}
function hasESManual(raw){
  // ES ... Manual  OU  Manual ... ES
  return /(^|[^A-Za-z])ES([^A-Za-z].*manual|$)|manual[^A-Za-z].*ES([^A-Za-z]|$)/i.test(raw);
}
function passUFStrict(it, uf){
  if(!uf) return true;
  if(it.ufs.has(uf)) return true;
  if(uf==="es" && (hasESManual(it.nameRaw)||hasESManual(it.urlRaw))) return true;
  return hasUFStrong(it.nameRaw, uf) || hasUFStrong(it.urlRaw, uf);
}

/* =========================
   Indexação
   ========================= */
function buildIndex(rows){
  const seen=new Set(), out=[];
  for(const r of rows){
    if(!r || !r.name || !r.url) continue;
    let url = String(r.url).trim();
    if(/^http:\/\//i.test(url)) url = url.replace(/^http:\/\//i, "https://"); // força https
    if(!/^https?:\/\/[^\s]+$/i.test(url)) continue;
    if(seen.has(url)) continue; seen.add(url);

    const nameRaw = String(r.name);
    const urlRaw  = url;

    const nameN = norm(nameRaw);
    const urlN  = norm(urlRaw);
    const slug  = wordsSlug(nameRaw + " " + urlRaw);
    const kws   = new Set(tokenize(nameRaw).concat(tokenize(urlRaw)));

    // UFs reconhecidas
    const ufs = new Set();
    for(const [uf, alts] of Object.entries(UF_MAP)){
      const altsN = [uf, ...alts.map(norm)];
      if(altsN.some(a => containsWord(slug, a))) ufs.add(uf);
      else if(hasUFStrong(nameRaw, uf) || hasUFStrong(urlRaw, uf)) ufs.add(uf);
    }
    if(hasESManual(nameRaw) || hasESManual(urlRaw)) ufs.add("es"); // reforço ES-Manual

    // cidades
    const cities = new Set();
    for(const [base, alts] of Object.entries(CITY_ALIASES)){
      const all = [base, ...alts.map(norm)];
      if(all.some(a => containsPhrase(slug, a))) cities.add(base);
    }

    out.push({
      name:r.name, url:urlRaw,
      nameN, urlN, slug, kws, ufs, cities,
      dscore: dateScore({nameN}),
      nameRaw, urlRaw
    });
  }
  return out;
}

/* =========================
   Expansão de consulta
   ========================= */
function expandQuery(q){
  const qn = norm(q);
  const parts = tokenize(qn);

  // Detecta UF e considera “apenas UF” quando TODOS os tokens são aliases da UF
  let uf = null;
  for(const [k, alts] of Object.entries(UF_MAP)){
    const aliasTokens = new Set([k, ...alts.flatMap(a => tokenize(a))]);
    const allFromUF   = parts.length>0 && parts.every(t => aliasTokens.has(t));
    if(allFromUF || alts.some(a => qn.includes(norm(a)))){ uf = k; break; }
  }
  if(uf && parts.every(t => new Set([uf, ...UF_MAP[uf].flatMap(a => tokenize(a))]).has(t))){
