"""Z9: MongoDB toplu sonuclarini 30 saniyede bir okuyan yerel pano.

Python standart HTTP sunucusu + mevcut PyMongo. Harici JS/CDN gerektirmez.
Calistirma icin compose.iot-pano.yml kullanilir. Veriye yazmaz.
"""
import json
import os
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit
from pymongo import MongoClient
from iot_mongo_sorgu_olcumu import build_queries, read_query

HTML = r'''<!doctype html>
<html lang="tr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>IoT Sensör Analizi</title>
<style>
*{box-sizing:border-box}
body{margin:0;background:#fff;color:#222;font-family:Arial,Helvetica,sans-serif;font-size:14px}
header{max-width:1160px;margin:24px auto 0;padding:0 20px 16px;border-bottom:2px solid #345d82}
header .eyebrow{font-size:13px;color:#555}
h1{font-size:25px;color:#244f75;margin:10px 0;font-weight:600}
header p{margin:0;color:#555;line-height:1.5}
main{max-width:1160px;margin:auto;padding:18px 20px}
.toolbar{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:18px}
button,select{font:inherit;padding:7px 10px;border:1px solid #aaa;border-radius:3px;background:#f7f7f7;color:#222}
button{cursor:pointer}button:hover{background:#e8edf2}button:disabled{opacity:.5;cursor:wait}
.badge{font-size:13px;color:#555}.muted{font-size:12px;color:#666}
.error{padding:12px;background:#fff0f0;color:#942828;border:1px solid #ddb9b9;margin:12px 0;line-height:1.5}
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}
.card{padding:14px;border:1px solid #ccc;background:#f8f9fa}
.card .label{font-size:13px;color:#444}.card .value{font-size:24px;font-weight:600;margin:8px 0;color:#244f75}.card .detail{font-size:12px;color:#666}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:20px}
.panel{padding:16px;border:1px solid #ccc;min-width:0;background:#fff}
.panel.wide{grid-column:1/-1}
h2{font-size:17px;color:#244f75;margin:0 0 9px;font-weight:600}
.sub{font-size:12px;color:#666;line-height:1.5;margin:0 0 14px}
svg{display:block;width:100%;height:auto}
svg text{font-family:Arial,Helvetica,sans-serif;fill:#555;font-size:12px}
svg .axis{stroke:#ddd;stroke-width:1}
.tablewrap{overflow:auto}table{border-collapse:collapse;width:100%;font-size:13px}
th,td{text-align:right;padding:9px 8px;border:1px solid #ddd;white-space:nowrap}
th:first-child,td:first-child{text-align:left}th{font-weight:600;background:#edf1f5;color:#333}
.source{margin-top:22px;padding:14px;background:#f6f6f6;border:1px solid #ccc;font-size:12px;line-height:1.7;color:#555;overflow-wrap:anywhere}
.source strong{color:#333}.source p{margin:4px 0}footer{font-size:12px;color:#666;margin:20px 0}#loaded{display:none}
@media(max-width:850px){.cards{grid-template-columns:1fr 1fr}.grid{grid-template-columns:1fr}.panel.wide{grid-column:auto}header{margin-top:16px}h1{font-size:22px}}
@media(max-width:420px){.cards{grid-template-columns:1fr}.toolbar{align-items:stretch}}
</style></head><body>
<header><div class="eyebrow">Büyük Veri Projesi — Şablon 3 / IoT</div><h1>IoT Sensör Analizi</h1><p>Grup: G211210553 | Toplu işlem sonuçları</p></header>
<main><div class="toolbar"><span class="badge">Toplu veri görünümü — akış henüz bağlı değil</span><button id="refresh">Şimdi yenile</button><span class="muted" id="status" role="status" aria-live="polite">Veriler yükleniyor…</span></div>
<div id="error" class="error" role="alert" hidden></div>
<div id="loaded"><div class="cards">
<section class="card"><div class="label">Temiz kaynak olayı</div><div class="value" id="count">—</div><div class="detail">Delta tablosundaki kayıt sayısı</div></section>
<section class="card"><div class="label">Analizdeki sensör</div><div class="value" id="sensors">—</div><div class="detail">Model gruplarının sensör toplamı</div></section>
<section class="card"><div class="label">Anomali işaretli olay</div><div class="value" id="anomalies">—</div><div class="detail" id="threshold">—</div></section>
<section class="card"><div class="label">Veri tarih aralığı · UTC</div><div class="value" id="period" style="font-size:18px;line-height:1.7">—</div><div class="detail">Pano açılış tarihi değildir</div></section>
</div><div class="grid">
<section class="panel wide"><h2>Model bazında anomali oranı</h2><p class="sub">Her modelde anomali işaretli olay / toplam olay (%) · sensör oranlarının ortalamasından farklıdır.</p><svg id="modelChart" viewBox="0 0 900 260" role="img" aria-label="Sensör modellerinin anomali yüzdeleri"></svg></section>
<section class="panel wide"><div class="toolbar" style="margin-bottom:6px"><h2 style="margin-right:auto">Bölgeye göre saatlik desen</h2><label for="region">Bölge</label><select id="region"></select></div><p class="sub">Tüm veri günleri birlikte değerlendirilir. X ekseni UTC saattir; gün gün zaman serisi veya canlı sensör akışı değildir.</p><div class="grid">
<div><h2 style="font-size:14px">Ortalama sıcaklık (°C)</h2><svg id="tempChart" viewBox="0 0 620 260" role="img" aria-label="Seçili bölgenin saatlik sıcaklık deseni"></svg></div>
<div><h2 style="font-size:14px">Ortalama nem (%)</h2><p class="sub">Sabit %0–100 ekseni; küçük farkların büyütülmesini önler.</p><svg id="humidityChart" viewBox="0 0 620 260" role="img" aria-label="Seçili bölgenin saatlik nem deseni, yüzde 0 ile 100 arasında sabit eksen"></svg></div>
</div></section>
<section class="panel"><h2>Kurulum yılı ve anomali</h2><p class="sub">Her sensörün anomali yüzdesi hesaplanır, sensör yüzdelerinin eşit ağırlıklı ortalaması alınır.</p><div class="tablewrap"><table><thead><tr><th>Kurulum yılı</th><th>Sensör</th><th>Ortalama oran (%)</th></tr></thead><tbody id="years"></tbody></table></div></section>
<section class="panel"><h2>Gece–gündüz karşılaştırması</h2><p class="sub">Gündüz 06.00–17.59 UTC; gece diğer saatler. Farklar yuvarlanmamış ortalamalardan hesaplanır.</p><div class="tablewrap"><table><thead><tr><th>Bölge</th><th>Gündüz °C</th><th>Gece °C</th><th>Fark °C</th></tr></thead><tbody id="periods"></tbody></table></div></section>
</div><section class="source" aria-label="Veri kaynağı ve yenilenme bilgisi"><p><strong>Veri kaynağı:</strong> MongoDB / bsm461_iot · <span id="sourceCount">—</span></p><p><strong>Kaynak Delta:</strong> <span id="source">—</span></p><p><strong>Aktif toplu koşu:</strong> <span id="run">—</span></p><p><strong>Sonuç üretimi:</strong> <span id="produced">—</span> · <strong>MongoDB yayını:</strong> <span id="published">—</span></p><p><strong>MongoDB'den son okuma:</strong> <span id="read">—</span> · <strong>Tarayıcıda görüntüleme:</strong> <span id="render">—</span></p><p>Pano 30 saniyede bir MongoDB'yi tekrar okur. Toplu iş otomatik yeniden çalıştırılmaz; yeni tamamlanmış bir koşu yayımlanınca grafikler güncellenir. Geçmiş veri, yalnızca ekran yenilendiği için canlı veri sayılmaz.</p></section>
</div><footer>Veriler MongoDB’den okunur. Anomali sayıları, belirlenen eşiklerin üzerindeki olayları gösterir.</footer></main>
<script>
'use strict';
const $=id=>document.getElementById(id), ns='http://www.w3.org/2000/svg';
let data=null,busy=false;
const number=(n,d=0)=>Number(n).toLocaleString('tr-TR',{minimumFractionDigits:d,maximumFractionDigits:d});
function set(id,text){$(id).textContent=text;}
function date(value){if(!value)return '—';const x=new Date(value);return Number.isNaN(x.getTime())?String(value):x.toLocaleString('tr-TR',{timeZoneName:'short'});}
function el(tag,attrs,parent,text){const n=document.createElementNS(ns,tag);for(const [k,v]of Object.entries(attrs))n.setAttribute(k,String(v));if(text!==undefined)n.textContent=text;parent.appendChild(n);return n;}
function chart(id,rows,key,label,color,bar=false,domain=null){
 const svg=$(id),w=svg.viewBox.baseVal.width,h=260,p={l:58,r:20,t:24,b:42};svg.replaceChildren();
 const valid=rows.filter(r=>Number.isFinite(Number(r[key]))&&r[key]!==null);
 if(!valid.length){el('text',{x:60,y:70},svg,'Bu seçim için veri yok.');return;}
 const values=valid.map(r=>Number(r[key]));let lo=bar?0:Math.min(...values),hi=Math.max(...values);
 if(domain){[lo,hi]=domain;}else if(hi===lo){lo-=1;hi+=1;}else if(!bar){const pad=(hi-lo)*.12;lo-=pad;hi+=pad;}else hi*=1.15;
 const cw=w-p.l-p.r,ch=h-p.t-p.b,sy=v=>h-p.b-(v-lo)/(hi-lo)*ch;
 for(let i=0;i<=4;i++){const v=lo+(hi-lo)*i/4,y=sy(v);el('line',{x1:p.l,x2:w-p.r,y1:y,y2:y,class:'axis'},svg);el('text',{x:p.l-10,y:y+4,'text-anchor':'end'},svg,number(v,1));}
 if(bar){const step=cw/valid.length,bw=Math.min(step*.58,100);valid.forEach((r,i)=>{const x=p.l+step*(i+.5);const node=el('rect',{x:x-bw/2,y:sy(r[key]),width:bw,height:Math.max(0,sy(0)-sy(r[key])),rx:4,fill:color},svg);el('title',{},node,label(r)+': '+number(r[key],2)+'%');el('text',{x,y:sy(r[key])-8,'text-anchor':'middle'},svg,number(r[key],2)+'%');el('text',{x,y:h-17,'text-anchor':'middle'},svg,label(r));});}
 else{const sx=i=>p.l+i*cw/Math.max(1,valid.length-1);el('polyline',{points:valid.map((r,i)=>sx(i)+','+sy(r[key])).join(' '),fill:'none',stroke:color,'stroke-width':3,'stroke-linejoin':'round'},svg);valid.forEach((r,i)=>{const c=el('circle',{cx:sx(i),cy:sy(r[key]),r:3,fill:color},svg);el('title',{},c,label(r)+': '+number(r[key],2));if(i%4===0||i===valid.length-1)el('text',{x:sx(i),y:h-17,'text-anchor':'middle'},svg,label(r));});}
}
function table(id,rows,fields){const body=$(id);body.replaceChildren();for(const row of rows){const tr=document.createElement('tr');for(const f of fields){const td=document.createElement('td');td.textContent=f[1]!==null?number(row[f[0]],f[1]):String(row[f[0]]);tr.appendChild(td);}body.appendChild(tr);}}
function renderRegion(){if(!data)return;const rows=data.sonuclar.tum_bolgeler_saatlik.filter(r=>r.bolge===$('region').value).sort((a,b)=>a.saat-b.saat);chart('tempChart',rows,'ortalama_sicaklik',r=>String(r.saat).padStart(2,'0'),'#ad615a');chart('humidityChart',rows,'ortalama_nem',r=>String(r.saat).padStart(2,'0'),'#4e79a0',false,[0,100]);}
function render(payload){
 data=payload;const m=data.meta,s=data.sonuclar;set('count',number(m.kaynak_kayit));set('sensors',number(s.model_sensor_ozet.reduce((sum,r)=>sum+r.sensor_sayisi,0)));set('anomalies',number(s.model_anomali.reduce((sum,r)=>sum+r.toplam_anomali,0)));
 set('threshold','Sıcaklık > '+number(m.esikler.SICAKLIK_ESIGI,2)+' °C veya titreşim > '+number(m.esikler.TITRESIM_ESIGI,3));set('period',m.veri_baslangici+' → '+m.veri_bitisi);
 chart('modelChart',s.model_anomali,'anomali_orani',r=>r.model,'#4e79a0',true);
 const prior=$('region').value,regions=[...new Set(s.tum_bolgeler_saatlik.map(r=>r.bolge))].sort((a,b)=>a.localeCompare(b,'tr'));
 $('region').replaceChildren();for(const r of regions){const option=document.createElement('option');option.value=r;option.textContent=r;$('region').appendChild(option);}if(regions.includes(prior))$('region').value=prior;renderRegion();
 table('years',s.yil_sensor_ozet,[['kurulum_yili',null],['sensor_sayisi',0],['ortalama_sensor_anomali_orani',2]]);
 table('periods',s.gece_gunduz,[['bolge',null],['gunduz_ort_sicaklik',2],['gece_ort_sicaklik',2],['sicaklik_farki',2]]);
 set('sourceCount',number(Object.values(m.sonuc_sayilari).reduce((a,b)=>a+b,0))+' özet belge / 5 sonuç koleksiyonu');
 set('source',m.kaynak+' · sürüm '+m.kaynak_surumu);set('run',m.kosu_id);set('produced',date(m.uretim_zamani));set('published',date(m.yayin_zamani));set('read',date(data.okuma_zamani));set('render',date(new Date().toISOString()));$('loaded').style.display='block';
}
async function refresh(){if(busy)return;busy=true;$('refresh').disabled=true;set('status','MongoDB okunuyor…');const abort=new AbortController(),timeout=setTimeout(()=>abort.abort(),20000);
 try{const response=await fetch('/api/toplu',{cache:'no-store',signal:abort.signal});const payload=await response.json();if(!response.ok)throw new Error(payload.hata||'Veri alınamadı.');render(payload);$('error').hidden=true;set('status','Sonuçlar yüklendi. Yenileme: 30 sn');}
 catch(error){$('error').hidden=false;$('error').textContent='MongoDB okuması başarısız. '+(data?'Son başarılı görüntü ekranda tutuluyor; güncel kabul etmeyin. ':'')+error.message;set('status','Bağlantı / veri hatası · 30 sn sonra yeniden denenecek');}
 finally{clearTimeout(timeout);busy=false;$('refresh').disabled=false;}}
$('refresh').addEventListener('click',refresh);$('region').addEventListener('change',renderRegion);refresh();setInterval(refresh,30000);
</script></body></html>'''

CLIENT = MongoClient(os.environ.get("MONGO_URI", "mongodb://mongo:27017"),
                     serverSelectionTimeoutMS=4000, socketTimeoutMS=10000, tz_aware=True)
DB = CLIENT["bsm461_iot"]


def load_data():
    start = time.perf_counter()
    meta = DB.servis_meta.find_one({"_id": "aktif_toplu"})
    if not meta or meta.get("durum") != "tamamlandi":
        raise RuntimeError("Tamamlanmis aktif toplu kosu yok. Once toplu sonuclari MongoDB'ye aktarin.")
    # Kimlik bir kere okunur: bu yenilemede tum grafikler ayni kosudan gelir.
    run_id = meta["kosu_id"]
    region = DB.bolge_saatlik.find_one({"kosu_id": run_id}, sort=[("bolge", 1)])
    if not region:
        raise RuntimeError("Aktif kosunun saatlik sonuclari bulunamadi.")
    queries = build_queries(run_id, region["bolge"])
    # Metadata ve secili bolge ek sorgulari gereksiz; tum bolgeler zaten okunur.
    results = {}
    for query in queries:
        if query["ad"] in ("aktif_kosu", "secili_bolge_saatlik"):
            continue
        rows = read_query(DB, query)
        if len(rows) != meta["sonuc_sayilari"][query["koleksiyon"]]:
            raise RuntimeError("Aktif kosunun sonuc sayimi tutarsiz: " + query["ad"])
        results[query["ad"]] = rows
    return {"meta": {key: meta[key] for key in (
                "kosu_id", "kaynak", "kaynak_surumu", "kaynak_kayit", "veri_baslangici",
                "veri_bitisi", "uretim_zamani", "yayin_zamani", "esikler", "sonuc_sayilari")},
            "sonuclar": results, "okuma_zamani": datetime.now(timezone.utc).isoformat(),
            "api_okuma_ms": (time.perf_counter() - start) * 1000, "yenileme_saniye": 30,
            "kaynak_turu": "toplu", "akis_bagli": False}


def serialize(value):
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError("JSON'a donusturulemeyen deger: " + type(value).__name__)


class Handler(BaseHTTPRequestHandler):
    def respond(self, status, content, kind):
        self.send_response(status)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "SAMEORIGIN")
        self.end_headers()
        self.wfile.write(content)

    def json_response(self, status, value):
        self.respond(status, json.dumps(value, ensure_ascii=False, default=serialize).encode("utf-8"),
                     "application/json; charset=utf-8")

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == "/":
            self.respond(200, HTML.encode("utf-8"), "text/html; charset=utf-8")
        elif path == "/api/toplu":
            try:
                self.json_response(200, load_data())
            except Exception as exc:
                self.json_response(503, {"hata": str(exc)[:500]})
        elif path == "/health":
            try:
                CLIENT.admin.command("ping")
                self.json_response(200, {"mongo": "ok"})
            except Exception:
                self.json_response(503, {"mongo": "erisilemiyor"})
        else:
            self.json_response(404, {"hata": "Bulunamadi"})


if __name__ == "__main__":
    print("IoT pano: 0.0.0.0:8050 | MongoDB toplu sonuclar | yenileme: 30 sn", flush=True)
    server = ThreadingHTTPServer(("0.0.0.0", 8050), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        CLIENT.close()
