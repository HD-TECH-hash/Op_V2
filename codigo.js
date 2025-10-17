// codigo.js — CRION busca híbrida de alta precisão (somente index; zero alucinação)

// ===== UF =====
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

// ===== CIDADES (aliases robustos) =====
const CITY_ALIASES = {
  "sao cristovao": ["s.cristovao","s cristovao","sao-cristovao","sao cristóvão","s.cristovão","s cristovão"],
  "sao jose dos campos": ["sjc","s jose dos campos","s.jose dos campos"],
  "sao bernardo": ["s.bernardo","s bernardo","sao-bernardo","sao bernado"], // aceita erro 'bernado'
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
  "maranhao": ["ma"], "amazonas": ["am"], "sergipe": ["se"], "pernambuco": ["pe"],
  "para": ["pará","pa"]
};

// ===== TOKENS ESPECIAIS (mapas de precisão) =====
// 'samp' trava cidade São Bernardo + UF ES
const SPECIAL_CITY_TOKENS = {
  "samp": { city: "sao bernardo", uf: "es" }
};

// ===== Normalização e tokenização =====
const STOP = new Set(["de","da","do","das","dos","e","a","o","as","os","the"]);
const norm = s => String(s||"")
  .toLowerCase()
  .normalize("NFD")
  .replace(/\p{Diacritic}/gu,"")
  .replace(/[._]/g," ")
  .replace(/\s+/g," ")
  .trim();

const tokenize = s => norm(s)
  .replace(/[-/]/g," ")
  .replace(/[^\p{Letter}\p{Number}\s]/gu," ")
  .split(/\s+/)
  .filter(t => t && !STOP.has(t));

// ===== Marcas/domínios =====
const BRAND_DOMAINS = { affix: "affix.com.br", alter: "alter.com.br" };
const detectBrands = qn => ({ hasAffix: /\baffix\b/i.test(qn), hasAlter: /\balter\b/i.test(qn) });

// ===== Datas no nome → boost =====
function extractMY(nameN){
  const m = nameN.match(/[-_](0[1-9]|1[0-2])[-_](\d{2})(?=($|[^0-9]))/);
  if(!m) return null;
  return { year: 2000 + +m[2], month: +m[1] };
}
const dateScore = item => { const my = extractMY(item.nameN); return my ? my.year*12 + my.month : 0; };

// ===== helpers de match =====
const wordsSlug = s => ` ${tokenize(s).join(" ")} `;
const containsWord = (slug, t) => slug.includes(` ${t} `);
const containsPhrase = (slug, phrase) => {
  const p = tokenize(phrase).join(" ");
  return p && slug.includes(` ${p} `);
};
const countWholeWords = (item, terms) => {
  let c=0; for(const t of terms){ if(containsWord(item.slug,t) || item.kws.has(t)) c++; } return c;
};

function queryHasUF(qn, uf){
  const alts = new Set([uf, ...(UF_MAP[uf]||[]).map(norm)]);
  const parts = new Set(tokenize(qn));
  for(const a of alts){ if(parts.has(a) || qn.includes(a)) return true; }
  return false;
}

// ===== Monta índice =====
function buildIndex(rows){
  const seen = new Set(), out = [];
  for(const r of rows){
    if(!r || !r.name || !r.url) continue;
    let url = String(r.url).trim();
    if(/^http:\/\//i.test(url)) url = url.replace(/^http:\/\//i,"https://");
    if(!/^https?:\/\/[^\s]+$/i.test(url)) continue;
    if(seen.has(url)) continue; seen.add(url);

    const nameN = norm(r.name);
    const urlN  = norm(url);
    const slug  = wordsSlug(r.name + " " + url);
    const kws   = new Set(tokenize(r.name).concat(tokenize(url)));

    const ufs = new Set();
    for(const [uf,alts] of Object.entries(UF_MAP)){
      const altsN = [uf, ...alts.map(norm)];
      if(altsN.some(a => containsWord(slug, a))) ufs.add(uf);
    }

    const cities = new Set();
    for(const [base,alts] of Object.entries(CITY_ALIASES)){
      const all = [base, ...alts.map(norm)];
      if(all.some(a => containsPhrase(slug, a))) cities.add(base);
    }

    out.push({ name:r.name, url, nameN, urlN, slug, kws, ufs, cities, dscore: dateScore({nameN}) });
  }
  return out;
}

// ===== Expansão de consulta =====
function expandQuery(q){
  const qn    = norm(q);
  const parts = tokenize(qn);

  // 1) detectar UF em qualquer posição
  let uf = null;
  for(const [k, alts] of Object.entries(UF_MAP)){
    const all = [k, ...alts.map(norm)];
    if(all.some(a => parts.includes(a) || qn.includes(a))) { uf = k; break; }
  }

  // 2) trava por cidade (frase)
  for(const [base, alts] of Object.entries(CITY_ALIASES)){
    const all = [base, ...alts.map(norm)];
    if(all.some(a => qn.includes(a))){
      const terms = new Set([...tokenize(base), ...(uf ? [uf] : [])]);
      return { terms, uf, cityLock: base };
    }
  }

  // 3) token especial (ex.: 'samp' → São Bernardo + ES)
  for(const [tok, rule] of Object.entries(SPECIAL_CITY_TOKENS)){
    if(parts.includes(tok)){
      const lockUF = uf || rule.uf || null;
      // se o usuário citou outro UF diferente, não aplica regra
      if(lockUF && uf && uf !== rule.uf) break;
      return {
        terms: new Set([tok, ...tokenize(rule.city), ...(lockUF? [lockUF] : [])]),
        uf: lockUF,
        cityLock: rule.city
      };
    }
  }

  // 4) aliases de cidade por token → só adiciona termos
  const extra = [];
  for(const [base, alts] of Object.entries(CITY_ALIASES)){
    const all = new Set([base, ...alts.map(norm)]);
    for(const t of parts){ if(all.has(t)) { extra.push(base); break; } }
  }

  // 5) termos finais (+ UF se houver)
  const terms = new Set([...parts, ...extra, ...(uf? [uf] : [])]);
  return { terms, uf };
}

// ===== Busca =====
function search(index, q){
  if(!index || !index.length) return [];
  const qOrig = String(q||"").trim(); if(!qOrig) return [];

  const qn = norm(qOrig);
  const {hasAffix, hasAlter} = detectBrands(qn);
  const {terms, uf, cityLock} = expandQuery(qOrig);
  const brandFilter = hasAffix || hasAlter;

  const passBrand = it =>
    !brandFilter ||
    (hasAffix && it.url.includes(BRAND_DOMAINS.affix)) ||
    (hasAlter && it.url.includes(BRAND_DOMAINS.alter));

  const passUF = it => !uf || it.ufs.has(uf) || queryHasUF(it.nameN, uf) || queryHasUF(it.urlN, uf);
  const passCityLock = it => !cityLock || containsPhrase(it.slug, cityLock);

  // 1) frase exata no name/url
  const exact = [];
  for(const it of index){
    if(!passBrand(it) || !passUF(it) || !passCityLock(it)) continue;
    if(it.nameN.includes(qn) || it.urlN.includes(qn)){
      exact.push({it, score: 1000 + it.dscore});
    }
  }
  if(exact.length){
    return exact.sort((a,b)=> b.score-a.score || a.it.name.localeCompare(b.it.name))
                .map(x=>({name:x.it.name, url:x.it.url}));
  }

  // 2) AND estrito por palavra inteira (todos os termos requeridos)
  const strict = [];
  for(const it of index){
    if(!passBrand(it) || !passUF(it) || !passCityLock(it)) continue;

    let ok = true;
    for(const t of terms){
      if(!containsWord(it.slug, t) && !it.kws.has(t)){ ok = false; break; }
    }
    if(!ok) continue;

    let bonus = 0;
    if(uf && it.ufs.has(uf)) bonus += 50;
    if(cityLock) bonus += 80;

    const c = countWholeWords(it, terms);
    strict.push({it, score: 400 + c*10 + bonus + it.dscore/100});
  }
  if(strict.length){
    return strict.sort((a,b)=> b.score-a.score || a.it.name.localeCompare(b.it.name))
                 .map(x=>({name:x.it.name, url:x.it.url}));
  }

  // 3) fallback parcial só se 1 termo, sem UF e sem cidade
  if(terms.size===1 && !uf && !cityLock){
    const soft = [];
    const t = [...terms][0];
    for(const it of index){
      if(!passBrand(it)) continue;
      let s = 0; if(it.nameN.includes(t)) s += 2; else if(it.urlN.includes(t)) s += 1;
      if(s>0) soft.push({it, score: 50 + s + it.dscore/200});
    }
    return soft.sort((a,b)=> b.score-a.score || a.it.name.localeCompare(b.it.name))
               .map(x=>({name:x.it.name, url:x.it.url}));
  }

  return [];
}

// ===== exporta (browser global) =====
window.buildIndex = buildIndex;
window.search     = search;
