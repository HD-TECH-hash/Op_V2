import {promises as fs} from "fs";
import fse from "fs-extra";
import path from "path";
import {fileURLToPath} from "url";
import {execFile} from "child_process";
import {createHash} from "crypto";
import globby from "globby";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DATA_DIRS = ["data/affix/raw", "data/alter/raw"];   // ajuste se precisar
const OUT_DIR = path.join(ROOT, "public");
const OUT_INDEX_DIR = path.join(OUT_DIR, "index");
const MANIFEST = path.join(OUT_DIR, "manifest.json");

function sha(s){ return createHash("md5").update(s).digest("hex").slice(0,10); }
function clean(t){
  return String(t||"")
    .replace(/\u00AD/g,"")
    .replace(/-\s*\n/g," ")
    .replace(/\r/g,"")
    .replace(/\n{2,}/g,"\n")
    .replace(/[ \t]{2,}/g," ")
    .trim();
}
function pdftotext(file){
  return new Promise((res,rej)=>{
    execFile("pdftotext", ["-layout","-enc","UTF-8","-q", file, "-"], {maxBuffer: 80*1024*1024},
      (err, stdout, stderr)=> err ? rej(err) : res(stdout.toString("utf8")));
  });
}

async function run(){
  await fse.ensureDir(OUT_INDEX_DIR);
  await fse.ensureFile(path.join(OUT_DIR, ".nojekyll"));

  // lista todos os PDFs
  const patterns = DATA_DIRS
    .map(d => path.join(ROOT, d, "**/*.pdf"));
  const files = await globby(patterns, {onlyFiles:true, expandDirectories:false});
  if(!files.length) throw new Error("Nenhum PDF encontrado em data/**/raw/");

  const items = [];
  for(const abs of files){
    const rel = path.relative(ROOT, abs).replace(/\\/g,"/");
    const base = path.basename(abs, ".pdf");
    const id = `${base}-${sha(rel)}`;                 // estável e único
    const outTxt = path.join(OUT_INDEX_DIR, `${id}.txt`);

    let text = "";
    try{
      text = await pdftotext(abs);
      if(!text || text.trim().length < 40) throw new Error("texto curto");
    }catch(_){
      // fallback: só marca vazio, mas mantém entrada
      text = "";
    }
    await fs.writeFile(outTxt, clean(text), "utf8");

    const st = await fs.stat(abs);
    items.push({
      id,
      name: path.basename(abs),
      relpath: rel,
      bytes: st.size,
      mtime: st.mtime.toISOString(),
      txt: `index/${id}.txt`
    });
    process.stdout.write(`indexed: ${rel} -> ${id}.txt\n`);
  }

  // manifesta
  await fs.writeFile(MANIFEST, JSON.stringify({
    generatedAt: new Date().toISOString(),
    count: items.length,
    items
  }, null, 2), "utf8");

  console.log(`OK: ${items.length} PDFs indexados.`);
}

run().catch(e=>{ console.error(e); process.exit(1); });
