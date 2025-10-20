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
  "sao cristovao":["s.cristovao","s cristovao","sao-cristovao","sao cristóvão","s.cristovão","s cristovão"],
  "sao bernardo":["s.bernardo","s bernardo","sao-bernardo","sao bernado","samp","linha samp","sao bernardo espirito santo","sao bernardo es"],
  "sao jose dos campos":["sjc","s jose dos campos","s.jose dos campos"],
  "belo horizonte":["bh"], "rio de janeiro":["rj capital","rio"], "sao paulo":["sp capital","sampa"],
  "porto alegre":["poa"], "cuiaba":["cuiabá"], "goiania":["goiânia"], "joao pessoa":["joão pessoa"],
  "tres lagoas":["três lagoas"], "mossoro":["mossoró"], "uberlandia":["uberlândia"],
  "ribeirao preto":["ribeirão preto"], "vitoria de santo antao":["vitória de santo antão"]
};

/* ===== Alias direto Espírito Santo → arquivo específico ===== */
const DIRECT_ALIAS = {
  "espirito santo": "Affix-ES-Manual-Corretor-Sao-Bernardo-Samp-Linha-Samp-10-25B.pdf",
  "espírito santo": "Affix-ES-Manual-Corretor-Sao-Bernardo-Samp-Linha-Samp-10-25B.pdf"
};

/* ===== Tokens especiais ===== */
const SPECIAL_CITY_TOKENS = {
  "samp": { city:"sao bernardo", uf:"es" },
  "sao bernardo": { city:"sao bernardo", uf:"es" }
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

/* ===== Helpers ===== */
const wordsSlug = s => ` ${tokenize(s).join(" ")} `;
const containsWord = (slug,t)=>slug.includes(` ${t} `);
const containsPhrase=(slug,phrase)=>{ const p=tokenize(phrase).join(" "); return p && slug.includes(` ${p} `); };
function hasUFStrong(raw, uf){ return new RegExp(`(^|[^A-Za-z])${uf.toUpperCase()}([^A-Za-z]|$)`).test(raw); }
function hasESManual(raw){ return /(^|[^A-Za-z])ES([^A-Za-z].*manual|$)|manual[^A-Za-z].*ES([^A-Za-z]|$)/i.test(raw); }
function sampBernardoSlug(slug){ return containsWord(slug,"samp") && containsWord(slug,"sao") && containsWord(slug,"bernardo"); }

/* ===== Força UF ===== */
function passUFStrict(it, uf){
  if(!uf) return true;
  if(it.ufs.has(uf)) return true;
  if(uf==="es" && (hasESManual(it.nameRaw)||hasESManual(it.urlRaw)||sampBernardoSlug(it.slug))) return true;
  return hasUFStrong(it.nameRaw, uf) || hasUFStrong(it.urlRaw, uf);
}

/* ===== Index ===== */
function buildIndex(rows){
  const seen=new Set(), out=[];
  for(const r of rows){
    if(!r||!r.name||!r.url) continue;
    let url=String(r.url).trim();
    if(/^http:\/\//i.test(url)) url=url.replace(/^http:\/\//i,"https://");
    if(!/^https?:\/\/[^\s]+$/i.test(url)) continue;
    if(seen.has(url)) continue; seen.add(url);

    const nameRaw=String(r.name), urlRaw=url;
    const nameN=norm(nameRaw), urlN=norm(urlRaw);
    const slug=wordsSlug(nameRaw+" "+urlRaw);
    const kws=new Set(tokenize(nameRaw).concat(tokenize(urlRaw)));

    const ufs=new Set();
    for(const [uf,alts] of Object.entries(UF_MAP)){
      const altsN=[uf,...alts.map(norm)];
      if(altsN.some(a=>containsWord(slug,a))) ufs.add(uf);
      else if(hasUFStrong(nameRaw,uf)||hasUFStrong(urlRaw,uf)) ufs.add(uf);
    }
    if(hasESManual(nameRaw)||hasESManual(urlRaw)||sampBernardoSlug(slug)) ufs.add("es");

    const cities=new Set();
    for(const [base,alts] of Object.entries(CITY_ALIASES)){
      const all=[base,...alts.map(norm)];
      if(all.some(a=>containsPhrase(slug,a))) cities.add(base);
    }

    out.push({ name:r.name, url:urlRaw, nameN, urlN, slug, kws, ufs, cities, dscore:dateScore({nameN}), nameRaw, urlRaw });
  }
  return out;
}

/* ===== Expansão ===== */
function expandQuery(q){
  const qn=norm(q);
  const parts=tokenize(qn);

  // alias direto para Espírito Santo
  for(const key in DIRECT_ALIAS){
    if(qn.includes(key)) return { direct:DIRECT_ALIAS[key], uf:"es" };
  }

  let uf=null;
  for(const [k,alts] of Object.entries(UF_MAP)){
    const aliasTokens = new Set([k, ...alts.flatMap(a=>tokenize(a))]);
    if(parts.every(t=>aliasTokens.has(t))){ uf=k; break; }
  }

  if(!uf){
    const qHasSamp=parts.includes("samp");
    const qHasSB=qn.includes("sao bernardo")||parts.includes("bernardo");
    if(qHasSamp||qHasSB) uf="es";
  }

  let cityLock=null;
  for(const [base,alts] of Object.entries(CITY_ALIASES)){
    const all=[base,...alts.map(norm)];
    if(all.some(a=>qn.includes(a))){ cityLock=base; break; }
  }

  return {terms:new Set([...parts]), uf, cityLock};
}

/* ===== Busca ===== */
function search(index,q){
  if(!index?.length) return [];
  const qn=norm(q||""); if(!qn) return [];

  const {hasAffix,hasAlter}=detectBrands(qn);
  const {terms,uf,cityLock,direct}=expandQuery(q);
  const brandFilter=hasAffix||hasAlter;

  // se for alias direto (ex: espírito santo)
  if(direct){
    return index.filter(it=>it.name.includes(direct))
      .map(it=>({name:it.name,url:it.url}));
  }

  const passBrand=it=>!brandFilter||
    (hasAffix&&it.url.includes(BRAND_DOMAINS.affix))||
    (hasAlter&&it.url.includes(BRAND_DOMAINS.alter));
  const passCity=it=>!cityLock||containsPhrase(it.slug,cityLock);

  const results=[];
  for(const it of index){
    if(!passBrand(it)||!passUFStrict(it,uf)||!passCity(it)) continue;
    const ok=[...terms].some(t=>containsWord(it.slug,t)||it.kws.has(t));
    if(ok) results.push({it,score:500+terms.size*10+it.dscore/100});
  }

  return results.sort((a,b)=>b.score-a.score)
                .map(x=>({name:x.it.name,url:x.it.url}));
}

/* ===== Export ===== */
window.buildIndex=buildIndex;
window.search=search;
