// codigo.js — CRION busca de alta precisão (index local, zero alucinação)

/* ===== UFs ===== */
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

/* ===== Cidades e aliases ===== */
const CITY_ALIASES = {
  // São Cristovão — variações robustas
  "sao cristovao":[
    "sao-cristovao","sao cristóvão","são cristovao","são cristovão",
    "s.cristovao","s cristovao","s.cristovão","s cristovão","sao cristovão"
  ],
  // São Bernardo / Samp
  "sao bernardo":[
    "s.bernardo","s bernardo","sao-bernardo","sao bernado","samp","linha samp",
    "sao bernardo espirito santo","sao bernardo es"
  ],
  "sao jose dos campos":["sjc","s jose dos campos","s.jose dos campos"],
  "belo horizonte":["bh"], "rio de janeiro":["rj capital","rio"], "sao paulo":["sp capital","sampa"],
  "porto alegre":["poa"], "cuiaba":["cuiabá"], "goiania":["goiânia"], "joao pessoa":["joão pessoa"],
  "tres lagoas":["três lagoas"], "mossoro":["mossoró"], "uberlandia":["uberlândia"],
  "ribeirao preto":["ribeirão preto"], "vitoria de santo antao":["vitória de santo antão"]
};

/* ===== Aliases diretos (força retorno do arquivo alvo) ===== */
const DIRECT_ALIAS = {
  // Espírito Santo → Samp São Bernardo
  "espirito santo":"Affix-ES-Manual-Corretor-Sao-Bernardo-Samp-Linha-Samp-10-25B.pdf",
  "espírito santo":"Affix-ES-Manual-Corretor-Sao-Bernardo-Samp-Linha-Samp-10-25B.pdf",
  // São Cristovão → manual SP
  "sao cristovao":"Affix-SP-São Cristovao-Manual-Corretor-09-25.pdf",
  "são cristovão":"Affix-SP-São Cristovao-Manual-Corretor-09-25.pdf",
  "sao-cristovao":"Affix-SP-São Cristovao-Manual-Corretor-09-25.pdf"
};

/* ===== Tokens/heurísticas especiais ===== */
const SPECIAL_CITY_TOKENS = {
  "samp": { city:"sao bernardo", uf:"es" }
};

/* ===== Normalização ===== */
const STOP = new Set(["de","da","do","das","dos","e","a","o","as","os","the"]);
const norm = s => String(s||"").toLowerCase().normalize("NFD")
  .replace(/\p{Diacritic}/gu,"").replace(/[._]/g," ").replace(/\s+/g," ").trim();
const tokenize = s => norm(s).replace(/[-/]/g," ")
  .replace(/[^\p{Letter}\p{Number}\s]/gu," ").split(/\s+/).filter(t=>t && !STOP.has(t));

/* ===== Marcas ===== */
const BRAND_DOMAINS = { affix:"affix.com.br", alter:"alter.com.br" };
const detectBrands = qn => ({ hasAffix:/\baffix\b/i.test(qn), hasAlter:/\balter\b/i.test(qn) });

/* ===== Datas ===== */
function extractMY(nameN){
  const m = nameN.match(/[-_](0[1-9]|1[0-2])[-_](\d{2})(?=($|[^0-9]))/);
  return m ? {year:2000+ +m[2], month:+m[1]} : null;
}
const dateScore = item => { const my=extractMY(item.nameN); return my? my.year*12+my.month : 0; };

/* ===== Helpers de correspondência ===== */
const wordsSlug = s => ` ${tokenize(s).join(" ")} `;
const containsWord = (slug,t)=>slug.includes(` ${t} `);
const containsPhrase=(slug,phrase)=>{ const p=tokenize(phrase).join(" "); return p && slug.includes(` ${p} `); };

// frase “sao cristovao” com separadores flexíveis
const CRISTOVAO_RE = /(^|[^a-z])sao[\s\-_.]+cristovao([^a-z]|$)/;
function hasCristovaoPhrase(rawNorm){ return CRISTOVAO_RE.test(rawNorm); }

function hasUFStrong(raw, uf){
  return new RegExp(`(^|[^A-Za-z])${uf.toUpperCase()}([^A-Za-z]|$)`).test(raw);
}
function hasESManual(raw){
  return /(^|[^A-Za-z])ES([^A-Za-z].*manual|$)|manual[^A-Za-z].*ES([^A-Za-z]|$)/i.test(raw);
}

/* ===== Index ===== */
function buildIndex(rows){
  const seen=new Set(), out=[];
  for(const r of rows){
    if(!r||!r.name||!r.url) continue;
    let url=String(r.url).trim();
    if(/^http:\/\//i.test(url)) url=url.replace(/^http:\/\//i,"https://");
    if(!/^
