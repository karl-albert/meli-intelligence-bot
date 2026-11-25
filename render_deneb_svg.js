const fs = require('fs');
const path = require('path');

// Locate installed vega v5 module
const vegaPaths = [
  'C:/Users/karla/.gemini/antigravity/scratch/vega5_test/node_modules/vega',
  path.join(__dirname, 'node_modules', 'vega')
];
let vega = null;
let vegaRoot = null;
for (const p of vegaPaths) {
  if (fs.existsSync(p)) {
    vega = require(p);
    vegaRoot = path.dirname(p);
    break;
  }
}
if (!vega) {
  console.error('Modulo vega nao encontrado em:', vegaPaths);
  process.exit(1);
}

// Support Deneb pbiPatternSVG expression function
const registeredPatterns = new Map();
function registerPatternFunc(targetModule) {
  if (targetModule && targetModule.expressionFunction) {
    targetModule.expressionFunction('pbiPatternSVG', function (pattern, fg, bg) {
      const fgColor = fg || '#94A3B8';
      const bgColor = bg || 'transparent';
      const id = `pbi_pat_${pattern}_${fgColor.replace(/[^a-zA-Z0-9]/g, '')}_${bgColor.replace(/[^a-zA-Z0-9]/g, '')}`;
      if (!registeredPatterns.has(id)) {
        registeredPatterns.set(id, { pattern, fg: fgColor, bg: bgColor });
      }
      return `url(#${id})`;
    });
  }
}

registerPatternFunc(vega);
try {
  const parserFuncs = require(path.join(vegaRoot, 'vega-parser', 'node_modules', 'vega-functions'));
  registerPatternFunc(parserFuncs);
} catch (e) {
  // Ignore if not present
}

const [, , specPath, payloadPath, measureName, outSvgPath, themeArg, regionArg] = process.argv;
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
  if (registeredPatterns.size > 0) {
    let defsContent = '';
    for (const [id, pat] of registeredPatterns.entries()) {
      if (pat.pattern === 'diagonal-stripe-1') {
        defsContent += `<pattern id="${id}" patternUnits="userSpaceOnUse" width="8" height="8">` +
          (pat.bg !== 'transparent' ? `<rect width="8" height="8" fill="${pat.bg}"/>` : '') +
          `<path d="M-1,1 l2,-2 M0,8 l8,-8 M7,9 l2,-2" stroke="${pat.fg}" stroke-width="1.2" stroke-linecap="square"/>` +
          `</pattern>`;
      }
    }
    if (defsContent) {
      if (svg.includes('<defs>')) {
        svg = svg.replace('<defs>', `<defs>${defsContent}`);
      } else {
        svg = svg.replace('<svg', `<svg><defs>${defsContent}</defs>`);
      }
    }
  }
  fs.writeFileSync(outSvgPath, svg, 'utf8');
  console.log(`OK ${path.basename(outSvgPath)} [Theme: ${theme}] [Region: ${region}] (${svg.length} bytes)`);
}).catch(err => {
  console.error('ERRO Vega:', err);
  process.exit(1);
});
