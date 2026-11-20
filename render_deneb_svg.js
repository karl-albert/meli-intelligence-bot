const fs = require('fs');
const path = require('path');

// Locate installed vega v5 module
const vegaPaths = [
  'C:/Users/karla/.gemini/antigravity/scratch/vega5_test/node_modules/vega',
  path.join(__dirname, 'node_modules', 'vega')
];
let vega = null;
for (const p of vegaPaths) {
  if (fs.existsSync(p)) {
    vega = require(p);
    break;
  }
}
if (!vega) {
  console.error('Modulo vega nao encontrado em:', vegaPaths);
  process.exit(1);
}

const [,, specPath, payloadPath, measureName, outSvgPath, themeArg, regionArg] = process.argv;
if (!specPath || !payloadPath || !measureName || !outSvgPath) {
  console.error('Uso: node render_deneb_svg.js <spec.json> <payload.json> <measureName> <out.svg> [theme] [region]');
  process.exit(1);
}

const spec = JSON.parse(fs.readFileSync(specPath, 'utf8'));
const payloadStr = fs.readFileSync(payloadPath, 'utf8');
const theme = themeArg || 'Light';
const region = regionArg || 'Todas as Regiões';

// Inject dataset payload
const ds = (spec.data || []).find(d => d.name === 'dataset');
if (ds) {
  ds.values = [{ [measureName]: payloadStr }];
}

// Ensure Light theme default (or requested theme)
const themeSig = (spec.signals || []).find(s => s.name === 'selectedTheme');
if (themeSig) {
  themeSig.value = theme;
}

// Ensure selectedRegion (Todas as Regiões, Sudeste, Sul, Nordeste, Centro-Oeste, Norte)
const regionSig = (spec.signals || []).find(s => s.name === 'selectedRegion');
if (regionSig) {
  regionSig.value = region;
}

// Fix any known color typo in barProjGradient if present
const gradSig = (spec.signals || []).find(s => s.name === 'barProjGradient');
if (gradSig && typeof gradSig.update === 'string') {
  gradSig.update = gradSig.update.replace("color: '0F2942'", "color: '#0F2942'");
}

const runtime = vega.parse(spec);
const view = new vega.View(runtime, { renderer: 'none' }).initialize();

view.runAsync().then(() => view.toSVG()).then(svg => {
  fs.writeFileSync(outSvgPath, svg, 'utf8');
  console.log(`OK ${path.basename(outSvgPath)} [Region: ${region}] (${svg.length} bytes)`);
}).catch(err => {
  console.error('ERRO Vega:', err);
  process.exit(1);
});
