# -*- coding: utf-8 -*-
"""IoT ham JSONL verisini okur; kaynak veriyi degistirmez.
Calistirma: docker compose exec araclar python /kit/araclar/iot_veri_kontrol.py
Tekrar: satir anahtar sirasi onemsiz olacak sekilde tum JSON iceriginin ayniligi.
Gec veri orani hesaplanmaz: bu dosyalarda ulasma zamani yoktur.
"""
import csv
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
from collections import Counter
from datetime import datetime
from pathlib import Path

sys.path.insert(0, '/kit/araclar')
from ortak import s3

FIELDS = ['olay_zamani', 'sensor_id', 'bolge', 'sicaklik', 'nem', 'titresim']
OUT = Path('/kit/kanit')

def main():
    OUT.mkdir(parents=True, exist_ok=True)
    client = s3()
    objects = sorted(
        [o for page in client.get_paginator('list_objects_v2').paginate(Bucket='ham', Prefix='iot/')
         for o in page.get('Contents', []) if o['Key'].endswith('.jsonl')],
        key=lambda o: o['Key'])
    if not objects:
        raise RuntimeError('ham/iot/ altinda JSONL dosyasi bulunamadi.')
    count = Counter()
    missing, nulls, empty = Counter(), Counter(), Counter()
    sensors, regions = Counter(), Counter()
    examples = {}
    fd, dbpath = tempfile.mkstemp(prefix='iot-kontrol-', suffix='.sqlite')
    os.close(fd)
    db = sqlite3.connect(dbpath)
    db.execute('CREATE TABLE seen (digest BLOB PRIMARY KEY) WITHOUT ROWID')
    db.execute('PRAGMA synchronous=OFF')
    db.execute('PRAGMA journal_mode=OFF')
    try:
        for index, obj in enumerate(objects, 1):
            print(f"Okunuyor {index}/{len(objects)}: {obj['Key']}", flush=True)
            body = client.get_object(Bucket='ham', Key=obj['Key'])['Body']
            try:
                for line_no, line in enumerate(body.iter_lines(), 1):
                    if not line.strip():
                        count['bos_satir'] += 1
                        continue
                    count['toplam_dolu_satir'] += 1
                    try:
                        row = json.loads(line.decode('utf-8-sig'))
                        if not isinstance(row, dict):
                            raise ValueError('JSON nesne degil')
                    except (ValueError, UnicodeError):
                        count['gecersiz_json_veya_nesne'] += 1
                        examples.setdefault('gecersiz_json', {'dosya': obj['Key'], 'satir': line_no})
                        continue
                    count['gecerli_json_nesnesi'] += 1
                    canonical = json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
                    digest = hashlib.sha256(canonical.encode('utf-8')).digest()
                    inserted = db.execute('INSERT OR IGNORE INTO seen VALUES (?)', (digest,)).rowcount
                    if not inserted:
                        count['tekrar_fazla_kopya'] += 1
                        examples.setdefault('tekrar', row)
                    ms = [f for f in FIELDS if f not in row]
                    ns = [f for f in FIELDS if f in row and row[f] is None]
                    es = [f for f in FIELDS if isinstance(row.get(f), str) and not row[f].strip()]
                    missing.update(ms); nulls.update(ns); empty.update(es)
                    for label, values in [('eksik_alanli_kayit', ms), ('null_degerli_kayit', ns), ('bos_metinli_kayit', es)]:
                        if values:
                            count[label] += 1
                            examples.setdefault(label, row)
                    if ms or ns or es:
                        count['en_az_bir_eksik_null_bos_iceren'] += 1
                    bad = False
                    for field in ['sicaklik', 'nem', 'titresim']:
                        if field in row and row[field] is not None and not (
                            isinstance(row[field], (int, float)) and not isinstance(row[field], bool)):
                            bad = True
                    if bad:
                        count['sayisal_tipi_uyusmayan_kayit'] += 1
                        examples.setdefault('sayisal_tipi_uyusmayan', row)
                    value = row.get('olay_zamani')
                    if value is not None:
                        try:
                            datetime.fromisoformat(str(value).replace('Z', '+00:00'))
                        except ValueError:
                            count['zaman_bicimi_gecersiz_kayit'] += 1
                    if isinstance(row.get('sensor_id'), str) and row['sensor_id'].strip():
                        sensors[row['sensor_id']] += 1
                    if isinstance(row.get('bolge'), str) and row['bolge'].strip():
                        regions[row['bolge']] += 1
            finally:
                body.close()
            db.commit()
        metrics = {name: count[name] for name in [
            'toplam_dolu_satir', 'bos_satir', 'gecerli_json_nesnesi', 'gecersiz_json_veya_nesne',
            'tekrar_fazla_kopya', 'eksik_alanli_kayit', 'null_degerli_kayit', 'bos_metinli_kayit',
            'en_az_bir_eksik_null_bos_iceren', 'sayisal_tipi_uyusmayan_kayit', 'zaman_bicimi_gecersiz_kayit']}
        metrics.update(dosya_sayisi=len(objects), toplam_dosya_bayti=sum(o['Size'] for o in objects),
                       farkli_sensor_sayisi=len(sensors), farkli_bolge_sayisi=len(regions),
                       benzersiz_json_kaydi=count['gecerli_json_nesnesi']-count['tekrar_fazla_kopya'])
        note = ('Kayit sorun kategorileri ortusebilir; toplanmamalidir. Alan sayimlari tekrarlar dahil ham kayitlardir. '
                'Tekrar, kanonik JSON icerigi SHA-256 ozetiyle sayilir; ilk kopyadan sonraki kayitlar raporlanir. '
                'Olay zamani sirasi Kafka bolumleri arasinda genel sira vermez. '
                'Ulasma zamani olmadigindan gec veri orani hesaplanmamistir. '
                'Anormal olcumler bu kontrolde silinmez veya kirli sayilmaz. Kaynak dosyalar degistirilmemistir.')
        report = dict(olcumler=metrics, alan_bazli_eksik=dict(missing), alan_bazli_null=dict(nulls),
                      alan_bazli_bos_metin=dict(empty), sensor_sayimlari=dict(sensors),
                      bolge_sayimlari=dict(regions), ornekler=examples, notlar=note,
                      kaynaklar=[dict(anahtar=o['Key'], boyut_bayt=o['Size'], etag=o.get('ETag')) for o in objects])
        (OUT/'iot_veri_kalitesi.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        with (OUT/'iot_veri_kalitesi.csv').open('w', encoding='utf-8-sig', newline='') as f:
            writer=csv.writer(f); writer.writerow(['olcum', 'deger']); writer.writerows(metrics.items())
        print('\nKONTROL TAMAMLANDI', flush=True)
        for key, value in metrics.items():
            print(f'{key}: {value}', flush=True)
        print('\nSonuclar: kanit/iot_veri_kalitesi.json ve kanit/iot_veri_kalitesi.csv', flush=True)
        print(note, flush=True)
    finally:
        db.close()
        os.unlink(dbpath)

if __name__ == '__main__':
    main()
