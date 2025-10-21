// scripts/build-index.mjs
import {promises as fs} from 'fs';
import path from 'path';
import {execFile} from 'child_process';
import {createHash} from 'crypto';
import fetch from 'node-fetch';

const CSV = 'data/urls.csv';           // header: name,url
const OUT_DIR = 'public/index';
const MANIFEST = 'public/manifest.json';
const ALLOWED = [/^https:\/\/(www\.)?affix\.com\.br\//i, /^https:\/\/(www\.)?alter\.com\.br\//i];

function sha(s){ return createHash('sha1').update(s).digest('hex').slice(0,16); }
function clean(s){ return s.normalize('NFD').replace(/[\u0300-\u036f]/g,''); }

async function pdftotext(pdfBytes){
  await fs.mkdir('.tmp', {recursive:true});
  const pdf = path.join('.tmp', `${Date.now()}_${Math.random().toString(36).slice(2)}.pdf`);
  const txt = pdf.replace(/\.pdf$/i,'.txt');
  await fs.writeFile(pdf, pdfBytes);
  await new Promise((res,rej)=>execFile('pdftotext',['-layout','-enc','UTF-8',pdf,txt],e=>e?rej(e):res()));
  const out = await fs.readFile(txt,'utf8').catch(()=> '');
  await fs.rm(pdf,{force:true}); await fs.rm(txt,{force:true});
  return out;
}

async function run(){
  await fs.mkdir(OUT_DIR,{recursive:true});
  const raw = await fs.readFile(CSV,'utf8');
  const lines = raw.split(/\r?\n/).filter(Boolean).slice(1);

  const items = [];
  for(const line of lines){
    const [name,url] = line.split(',').map(s=>s?.trim());
    if(!name || !url) continue;
    if(!ALLOWED.some(rx=>rx.test(url))) continue;

    const id = sha(url);
    const txtPath = `${OUT_DIR}/${id}.txt`;

    let txt = '';
    try{
      const r = await fetch(url);
      if(r.ok){
        const buf = Buffer.from(await r.arrayBuffer());
        txt = await pdftotext(buf);
      }
    }catch(_){}

    if(!txt){ console.warn('falha:', url); continue; }

    txt = clean(txt).replace(/\u00AD/g,'').replace(/-\s*\n/g,'').replace(/\s{2,}/g,' ').trim();
    await fs.writeFile(txtPath, txt, 'utf8');
    items.push({ id, name, url, path:`index/${id}.txt`, txt:`index/${id}.txt` });
    process.stdout.write('.');
  }

  await fs.writeFile(MANIFEST, JSON.stringify({generatedAt:new Date().toISOString(), items}, null, 2));
  console.log(`\nok: ${items.length} itens`);
}
run().catch(e=>{ console.error(e); process.exit(1); });
