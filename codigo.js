// search.js — CRION busca híbrida (exato + flexível)

// ===== Mapa UF =====
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

// ===== Normalização =====
const norm = s => s.toLowerCase().normalize('NFD').replace(/\p{Diacritic}/gu,'').trim();

// ===== Monta índice =====
function buildIndex(rows){
  return rows.map(r=>{
    const slug = ` ${norm(r.name)} `;
    const kws = new Set(slug.split(/\W+/).filter(Boolean));
    for (const [uf,alts] of Object.entries(UF_MAP)){
      const altsN = alts.map(norm);
      if ([uf,...altsN].some(a=>slug.includes(` ${a} `))){
        altsN.forEach(a=>kws.add(a)); kws.add(uf);
      }
    }
    return {...r, slug, kws};
  });
}

// ===== Expansão da consulta =====
function expandQuery(q){
  const qn = norm(q);
  for (const [uf, alts] of Object.entries(UF_MAP)){
    const all = [uf, ...alts.map(norm)];
    if (all.includes(qn)) return new Set(all);
  }
  return new Set(qn.split(/\s+/).filter(Boolean));
}

// ===== Busca híbrida =====
function search(index, q){
  const qOrig = q;
  const terms = expandQuery(q);
  const qOrigN = norm(qOrig);

  const exact = new Map();
  for (const it of index){
    if (norm(it.name).includes(qOrigN) || norm(it.url).includes(qOrigN)){
      exact.set(it.url, {item:it, score:100});
    }
  }

  const flex = new Map();
  for (const it of index){
    let s = 0;
    for (const t of terms){ if (it.kws?.has(t) || it.slug.includes(` ${t} `)) s++; }
    if (s>0) flex.set(it.url, {item:it, score:s});
  }

  const merged = new Map([...flex, ...exact]);
  return [...merged.values()]
    .sort((a,b)=>b.score-a.score || a.item.name.localeCompare(b.item.name))
    .map(x=>x.item);
}
