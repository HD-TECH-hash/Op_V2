// codigo.js — CRION busca de alta precisão (index local, mínima alucinação)

/* ========= UF: sigla ↔ nome (com e sem acento) ========= */
const UF_MAP = {
  ac:["acre","ac"],
  al:["alagoas","al"],
  ap:["amapa","amapá","ap"],
  am:["amazonas","am"],
  ba:["bahia","ba"],
  ce:["ceara","ceará","ce"],
  df:["distrito federal","df","brasilia","brasília","brasilia df","brasília df"],
  es:["espirito santo","espírito santo","es"],
  go:["goias","goiás","go"],
  ma:["maranhao","maranhão","ma"],
  mt:["mato grosso","mt"],
  ms:["mato grosso do sul","ms"],
  mg:["minas gerais","mg"],
  pa:["para","pará","pa"],
  pb:["paraiba","paraíba","pb"],
  pr:["parana","paraná","pr"],
  pe:["pernambuco","pe"],
  pi:["piaui","piauí","pi"],
  rj:["rio de janeiro","rj"],
  rn:["rio grande do norte","rn"],
  rs:["rio grande do sul","rs"],
  ro:["rondonia","rondônia","ro"],
  rr:["roraima","rr"],
  sc:["santa catarina","sc"],
  sp:["sao paulo","são paulo","sp","capital sp","sp capital"],
  se:["sergipe","se"],
  to:["tocantins","to"]
};

/* ========= CIDADES e aliases (com/sem acento, hífen, cedilha) ========= */
const CITY_ALIASES = {
  "sao cristovao":[
    "são cristovao","sao cristovão","são cristovão",
    "s.cristovao","s cristovao","sao-cristovao","s cristovão","s.cristovão"
  ],
  "sao bernardo":[
    "são bernardo","s bernardo","s.bernardo","sao-bernardo","sao bernado","samp" // SAMP = linha São Bernardo (ES)
  ],
  "sao jose dos campos":[ "sjc","s jose dos campos","s.jose dos campos","são josé dos campos" ],
  "belo horizonte":[ "bh" ],
  "rio de janeiro":[ "rj capital","rio" ],
  "sao paulo":[ "são paulo","sp capital","sampa" ],
  "porto alegre":[ "poa" ],
  "cuiaba":[ "cuiabá" ],
  "goiania":[ "goiânia" ],
  "joao pessoa":[ "joão pessoa" ],
  "tres lagoas":[ "três lagoas" ],
  "mossoro":[ "mossoró" ],
  "uberlandia":[ "uberlândia" ],
  "ribeirao preto":[ "ribeirão preto" ],
  "vitoria de santo antao":[ "vitória de santo antão" ],
  "maranhao":[ "ma" ],
  "amazonas":[ "am" ],
  "sergipe":[ "se" ],
  "pernambuco":[ "pe" ],
  "para":[ "pará","pa" ]
};

/* ========= Token especial que “força” UF/cidade ========= */
const SPECIAL_CITY_TOKENS = {
  // “samp” no nome/consulta indica linha São Bernardo no ES
  "samp": { city:"sao bernardo", uf:"es" }
};

/* ========= Normalização (remove acento, til, cedilha etc.) ========= */
const STOP = new Set(["de","da","do","das","dos","e","a","o","as","os","the"]);
const norm = s => String(s||"")
  .toLowerCase()
  .normalize("NFD")                 // separa acentos
  .replace(/\p{Diacritic}/gu,"")    // remove acentos (ã→a, ç→c)
  .replace(/[._]/g," ")
  .replace(/\s+/g," ")
  .trim();

const tokenize = s => norm(s)
  .replace(/[-/]/g," ")             // hífen/barra viram espaço
  .replace(/[^\p{Letter}\p{Number}\s]/gu," ")
  .split(/\s+/)
  .filter(t=>t && !STOP.has(t));

/* ========= Marcas/domínios ========= */
const BRAND_DOMAINS = { affix:"affix.com.br", alter:"alter.com.br" };
const detectBrands = qn => ({ hasAffix:/\baffix\b/i.test(qn), hasAlter:/\balter\b/i.test(qn) });

/* ========= Data no nome → boost ========= */
function extractMY(nameN){
  const m = nameN.match(/[-_](0[1-9]|1[0-2])[-_](\d{2})(?=($|[^0-9]))/);
  return m ? {year:2000+ +m[2], month:+m[1]} : null;
}
const dateScore = item => { const my=extractMY(item.nameN); return my? my.year*12+my.month : 0; };

/* ========= Helpers de match ========= */
const wordsSlug   = s => ` ${tokenize(s).join(" ")} `;
const containsWord   = (slug,t)=> slug.includes(` ${t} `);
const containsPhrase = (slug,phrase)=>{
  const p = tokenize(phrase).join(" ");
  return p && slug.includes(` ${p} `);
};

/* UF forte: sigla isolada por não-letras (ex.: “-ES-”, “(ES)”) */
function hasUFStrong(raw, uf){
  const sig = uf.toUpperCase();
  const re = new RegExp(`(^|[^A-Za-z])${sig}([^A-Za-z]|$)`);
  return re.test(raw);
}
/* Regra extra: “ES-Manual” ou “Manual-ES” força ES */
function hasESManual(raw){
  return /(^|[^A-Za-z])ES([^A-Za-z].*manual|$)|manual[^A-Za-z].*ES([^A-Za-z]|$)/i.test(raw);
}
function passUFStrict(it, uf){
  if(!uf) return true;
  if(it.ufs.has(uf)) return true;
  if(uf==="es" && (hasESManual(it.nameRaw)||hasESManual(it.urlRaw))) return true;
  return hasUFStrong(it.nameRaw, uf) || hasUFStrong(it.urlRaw, uf);
}

/* ========= Indexação ========= */
function buildIndex(rows){
  const seen=new Set(), out=[];
  for(const r of rows){
    if(!r || !r.name || !r.url) continue;

    let url=String(r.url).trim();
    if(/^http:\/\//i.test(url)) url=url.replace(/^http:\/\//i,"https://");  // força https
    if(!/^https?:\/\/[^\s]+$/i.test(url)) continue;
    if(seen.has(url)) continue; seen.add(url);

    const nameRaw = String(r.name);
    const urlRaw  = url;

    const nameN = norm(nameRaw);
    const urlN  = norm(urlRaw);
    const slug  = wordsSlug(nameRaw+" "+urlRaw);
    const kws   = new Set(tokenize(nameRaw).concat(tokenize(urlRaw)));

    // marca UFs detectadas
    const ufs=new Set();
    for(const [uf,alts] of Object.entries(UF_MAP)){
      const altsN=[uf, ...alts.map(norm)];
      if(altsN.some(a=>containsWord(slug,a))) ufs.add(uf);
      else if(hasUFStrong(nameRaw,uf) || hasUFStrong(urlRaw,uf)) ufs.add(uf);
    }
    if(hasESManual(nameRaw) || hasESManual(urlRaw)) ufs.add("es"); // reforço ES-Manual

    // marca cidades detectadas
    const cities=new Set();
    for(const [base,alts] of Object.entries(CITY_ALIASES)){
      const all=[base, ...alts.map(norm)];
      if(all.some(a=>containsPhrase(slug,a))) cities.add(base);
    }

    out.push({ name:r.name, url:urlRaw, nameN, urlN, slug, kws, ufs, cities, dscore:dateScore({nameN}), nameRaw, urlRaw });
  }
  return out;
}

/* ========= Expansão de consulta (UF/cidade/aliases/tokens) ========= */
function expandQuery(q){
  const qn    = norm(q);
  const parts = tokenize(qn);

  // Detecta UF e considera “apenas UF” se todos tokens são aliases dessa UF
  let uf=null;
  for(const [k,alts] of Object.entries(UF_MAP)){
    const aliasTokens = new Set([k, ...alts.flatMap(a=>tokenize(a))]);
    const allFromUF   = parts.length>0 && parts.every(t=>aliasTokens.has(t));
    if(allFromUF || alts.some(a=>qn.includes(norm(a)))){ uf=k; break; }
  }
  if(uf){
    const aliasTokens = new Set([uf, ...UF_MAP[uf].flatMap(a=>tokenize(a))]);
    if(parts.every(t=>aliasTokens.has(t))) return {terms:new Set([uf]), uf}; // ex.: “espirito santo”
  }

  // Lock por cidade se a frase aparece
  for(const [base,alts] of Object.entries(CITY_ALIASES)){
    const all=[base,...alts.map(norm)];
    if(all.some(a=>qn.includes(a))){
      const tset = new Set([...tokenize(base), ...(uf?[uf]:[])]);
      return {terms:tset, uf, cityLock:base};
    }
  }

  // Tokens especiais
  for(const [tok,rule] of Object.entries(SPECIAL_CITY_TOKENS)){
    if(parts.includes(tok)){
      const lockUF = uf || rule.uf;
      return {terms:new Set([tok, ...tokenize(rule.city), lockUF]), uf:lockUF, cityLock:rule.city};
    }
  }

  // Expansão leve: se algum token for alias de cidade, adiciona base
  const extra=[];
  for(const [base,alts] of Object.entries(CITY_ALIASES)){
    const all = new Set([base,...alts.map(norm)]);
    for(const t of parts){ if(all.has(t)){ extra.push(base); break; } }
  }

  return {terms:new Set([...parts, ...extra, ...(uf?[uf]:[])]), uf};
}

/* ========= Busca ========= */
function search(index,q){
  if(!index?.length) return [];
  const qn = norm(q||""); if(!qn) return [];

  const {hasAffix,hasAlter} = detectBrands(qn);
  const {terms, uf, cityLock} = expandQuery(q);

  const brandFilter = hasAffix || hasAlter;
  const passBrand = it =>
    !brandFilter ||
    (hasAffix && it.url.includes(BRAND_DOMAINS.affix)) ||
    (hasAlter && it.url.includes(BRAND_DOMAINS.alter));
  const passCity = it => !cityLock || containsPhrase(it.slug, cityLock);

  // 1) frase exata no name/url
  const exact=[];
  for(const it of index){
    if(!passBrand(it) || !passUFStrict(it,uf) || !passCity(it)) continue;
    if(it.nameN.includes(qn) || it.urlN.includes(qn)){
      exact.push({it, score:1000 + it.dscore});
    }
  }
  if(exact.length){
    return exact
      .sort((a,b)=> b.score-a.score || a.it.name.localeCompare(b.it.name))
      .map(x=>({name:x.it.name, url:x.it.url}));
  }

  // 2) AND estrito de palavras inteiras
  const strict=[];
  for(const it of index){
    if(!passBrand(it) || !passUFStrict(it,uf) || !passCity(it)) continue;
    const ok = [...terms].every(t => containsWord(it.slug,t) || it.kws.has(t));
    if(!ok) continue;
    strict.push({it, score:500 + terms.size*10 + it.dscore/100});
  }
  if(strict.length){
    return strict
      .sort((a,b)=> b.score-a.score || a.it.name.localeCompare(b.it.name))
      .map(x=>({name:x.it.name, url:x.it.url}));
  }

  // 3) sem resultados → nada (evita ruído)
  return [];
}

/* ========= Export (browser global) ========= */
window.buildIndex = buildIndex;
window.search     = search;
