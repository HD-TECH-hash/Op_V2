// codigo.js — CRION busca híbrida de alta precisão (index local → zero alucinação)

/* ========== UF MAP ========== */
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

/* ========== CITY MAP ========== */
const CITY_ALIASES = {
  "sao cristovao": ["s.cristovao","s cristovao","sao-cristovao","sao cristóvão","s.cristovão","s cristovão"],
  "sao jose dos campos": ["sjc","s jose dos campos","s.jose dos campos"],
  "sao bernardo": ["s.bernardo","s bernardo","sao-bernardo","sao bernado"],
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

/* ========== TOKENS ESPECIAIS ========== */
const SPECIAL_CITY_TOKENS = {
  "samp": { city: "sao bernardo", uf: "es" }
};

/* ========== NORMALIZAÇÃO ========== */
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

/* ========== MARCAS ========== */
const BRAND_DOMAINS = { affix:"affix.com.br", alter:"alter.com.br" };
const detectBrands = qn => ({ hasAffix:/\baffix\b/i.test(qn), hasAlter:/\balter\b/i.test(qn) });

/* ========== UTILITÁRIOS ========== */
function extractMY(nameN){
  const m = nameN.match(/[-_](0[1-9]|1[0-2])[-_](\d{2})(?=($|[^0-9]))/);
  return m ? {year:2000+ +m[2], month:+m[1]} : null;
}
const dateScore = item => { const my=extractMY(item.nameN); return my? my.year*12+my.month : 0; };

const wordsSlug = s => ` ${tokenize(s).join(" ")} `;
const containsWord = (slug,t)=>slug.includes(` ${t} `);
const containsPhrase=(slug,phrase)=>{
  const p=tokenize(phrase).join(" ");
  return p && slug.includes(` ${p} `);
};
function queryHasUF(textN, uf){
  const alts=new Set([uf,...(UF_MAP[uf]||[]).map(norm)]);
  const parts=new Set(tokenize(textN));
  for(const a of alts){ if(parts.has(a)||textN.includes(a)) return true; }
  return false;
}

/* ========== INDEXAÇÃO ========== */
function buildIndex(rows){
  const seen=new Set(), out=[];
  for(const r of rows){
    if(!r||!r.name||!r.url) continue;
    let url=String(r.url).trim();
    if(/^http:\/\//i.test(url)) url=url.replace(/^http:\/\//i,"https://");
    if(!/^https?:\/\/[^\s]+$/i.test(url)) continue;
    if(seen.has(url)) continue; seen.add(url);

    const nameN=norm(r.name);
    const urlN=norm(url);
    const slug=wordsSlug(r.name+" "+url);
    const kws=new Set(tokenize(r.name).concat(tokenize(url)));

    const ufs=new Set();
    for(const [uf,alts] of Object.entries(UF_MAP)){
      const altsN=[uf,...alts.map(norm)];
      if(altsN.some(a=>containsWord(slug,a))) ufs.add(uf);
    }

    const cities=new Set();
    for(const [base,alts] of Object.entries(CITY_ALIASES)){
      const all=[base,...alts.map(norm)];
      if(all.some(a=>containsPhrase(slug,a))) cities.add(base);
    }

    out.push({ name:r.name,url,nameN,urlN,slug,kws,ufs,cities,dscore:dateScore({nameN}) });
  }
  return out;
}

/* ========== EXPANSÃO DE CONSULTA ========== */
function expandQuery(q){
  const qn=norm(q);
  const parts=tokenize(qn);

  let uf=null, aliasSet=null;
  for(const [k,alts] of Object.entries(UF_MAP)){
    const alias=[k,...alts.map(norm)];
    if(alias.some(a=>parts.includes(a)||qn.includes(a))){
      uf=k; aliasSet=new Set(alias.map(norm)); break;
    }
  }

  // se a consulta for só UF (“espirito santo” → ES)
  if(uf){
    const onlyUF=[...parts].every(t=>aliasSet.has(t));
    if(onlyUF) return { terms:new Set([uf]), uf };
  }

  // trava cidade
  for(const [base,alts] of Object.entries(CITY_ALIASES)){
    const all=[base,...alts.map(norm)];
    if(all.some(a=>qn.includes(a)))
      return {terms:new Set([...tokenize(base),...(uf?[uf]:[])]),uf,cityLock:base};
  }

  // token especial “samp”
  for(const [tok,rule] of Object.entries(SPECIAL_CITY_TOKENS)){
    if(parts.includes(tok)){
      const lockUF=uf||rule.uf;
      return {terms:new Set([tok,...tokenize(rule.city),lockUF]),uf:lockUF,cityLock:rule.city};
    }
  }

  const extra=[];
  for(const [base,alts] of Object.entries(CITY_ALIASES)){
    const all=new Set([base,...alts.map(norm)]);
    for(const t of parts){ if(all.has(t)){ extra.push(base); break; } }
  }

  return {terms:new Set([...parts,...extra,...(uf?[uf]:[])]),uf};
}

/* ========== BUSCA ========== */
function search(index,q){
  if(!index?.length) return [];
  const qn=norm(q||"");
  if(!qn) return [];

  const {hasAffix,hasAlter}=detectBrands(qn);
  const {terms,uf,cityLock}=expandQuery(q);
  const brandFilter=hasAffix||hasAlter;

  const passBrand=it=>!brandFilter||
    (hasAffix&&it.url.includes(BRAND_DOMAINS.affix))||
    (hasAlter&&it.url.includes(BRAND_DOMAINS.alter));

  const passUF=it=>!uf||it.ufs.has(uf)||queryHasUF(it.nameN,uf)||queryHasUF(it.urlN,uf);
  const passCityLock=it=>!cityLock||containsPhrase(it.slug,cityLock);

  const exact=[];
  for(const it of index){
    if(!passBrand(it)||!passUF(it)||!passCityLock(it)) continue;
    if(it.nameN.includes(qn)||it.urlN.includes(qn))
      exact.push({it,score:1000+it.dscore});
  }
  if(exact.length)
    return exact.sort((a,b)=>b.score-a.score||a.it.name.localeCompare(b.it.name))
      .map(x=>({name:x.it.name,url:x.it.url}));

  const strict=[];
  for(const it of index){
    if(!passBrand(it)||!passUF(it)||!passCityLock(it)) continue;
    if([...terms].every(t=>containsWord(it.slug,t)||it.kws.has(t)))
      strict.push({it,score:500+terms.size*10+it.dscore/100});
  }
  if(strict.length)
    return strict.sort((a,b)=>b.score-a.score||a.it.name.localeCompare(b.it.name))
      .map(x=>({name:x.it.name,url:x.it.url}));

  return [];
}

/* ========== EXPORT ========== */
window.buildIndex=buildIndex;
window.search=search;
