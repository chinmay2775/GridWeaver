import { useEffect, useRef, useState } from 'react';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import './App.css';

const API = 'http://localhost:8080';

// index matches NodeType ordinal: SOLAR, BATTERY, LOAD
const TYPE_COLORS = ['#f5b301', '#4ade80', '#60a5fa'];
const TYPE_NAMES  = ['Solar', 'Battery', 'Load'];

const ZONE_BOUNDS = [
  { zone: 'A', lngLo: 73.750, lngHi: 73.798 },
  { zone: 'B', lngLo: 73.798, lngHi: 73.846 },
  { zone: 'C', lngLo: 73.846, lngHi: 73.894 },
  { zone: 'D', lngLo: 73.894, lngHi: 73.942 },
  { zone: 'E', lngLo: 73.942, lngHi: 73.990 },
];
const LAT_LO = 18.44, LAT_HI = 18.64;

export default function App() {
  const mapEl = useRef(null);
  const mapRef = useRef(null);
  const [status, setStatus] = useState('loading topology…');
  const [counts, setCounts] = useState(null);

  useEffect(() => {
    if (mapRef.current) return;

    // preferCanvas is the whole ballgame: 10k DOM markers will lock the tab,
    // 10k canvas circles render in one pass.
    const map = L.map(mapEl.current, { preferCanvas: true })
      .setView([18.54, 73.87], 12);
    mapRef.current = map;

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '© OpenStreetMap contributors © CARTO',
      maxZoom: 19,
    }).addTo(map);

    // zone columns
    ZONE_BOUNDS.forEach(({ zone, lngLo, lngHi }) => {
      L.rectangle([[LAT_LO, lngLo], [LAT_HI, lngHi]], {
        color: '#64748b', weight: 1, fillOpacity: 0.04, interactive: false,
      }).addTo(map);
      L.marker([LAT_HI + 0.008, (lngLo + lngHi) / 2], {
        icon: L.divIcon({ className: 'zone-label', html: zone }),
        interactive: false,
      }).addTo(map);
    });

    let cancelled = false;

    fetch(`${API}/api/topology`)
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(rows => {
        if (cancelled) return;

        const layer = L.layerGroup();
        const tally = [0, 0, 0];

        for (const [id, lat, lng, type] of rows) {
          tally[type]++;
          L.circleMarker([lat, lng], {
            radius: 3,
            stroke: false,
            fillColor: TYPE_COLORS[type],
            fillOpacity: 0.75,
          })
            .bindTooltip(`${id}<br>${TYPE_NAMES[type]}`, { direction: 'top' })
            .addTo(layer);
        }

        layer.addTo(mapRef.current);
        setCounts(tally);
        setStatus(`${rows.length.toLocaleString()} nodes`);
      })
      .catch(err => setStatus(`failed: ${err.message}`));

    return () => { cancelled = true; };
  }, []);

  return (
    <div className="shell">
      <div ref={mapEl} className="map" />
      <aside className="sidebar">
        <h1>GridWeaver</h1>
        <p className="status">{status}</p>
        {counts && (
          <ul className="legend">
            {TYPE_NAMES.map((name, i) => (
              <li key={name}>
                <span className="swatch" style={{ background: TYPE_COLORS[i] }} />
                {name}
                <span className="count">{counts[i].toLocaleString()}</span>
              </li>
            ))}
          </ul>
        )}
        <p className="note">Static topology.</p>
      </aside>
    </div>
  );
}