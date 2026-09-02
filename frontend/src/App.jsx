import { useEffect, useRef, useState } from 'react';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import './App.css';
import 'leaflet.heat/dist/leaflet-heat.js';

const API = 'http://localhost:8080';
const WS  = 'ws://localhost:8080/ws/grid';

const TYPE_NAMES = ['Solar', 'Battery', 'Load'];

// index = NodeStatus ordinal: IDLE, CHARGING, DISCHARGING, FAULT
const STATUS_COLORS = ['#475569', '#4ade80', '#fbbf24', '#ef4444'];
const STATUS_NAMES  = ['Idle', 'Charging', 'Discharging', 'Fault'];

const ZONE_BOUNDS = [
  { zone: 'A', lngLo: 73.750, lngHi: 73.798 },
  { zone: 'B', lngLo: 73.798, lngHi: 73.846 },
  { zone: 'C', lngLo: 73.846, lngHi: 73.894 },
  { zone: 'D', lngLo: 73.894, lngHi: 73.942 },
  { zone: 'E', lngLo: 73.942, lngHi: 73.990 },
];
const LAT_LO = 18.44, LAT_HI = 18.64;

export default function App() {
  const mapEl    = useRef(null);
  const mapRef   = useRef(null);
  const markers  = useRef([]);       // index-aligned with the backend node index
  const statuses = useRef(null);     // Int8Array of current status ordinals
  const flowRef = useRef(null);

  const [conn, setConn]     = useState('connecting');
  const [zones, setZones]   = useState([]);
  const [tally, setTally]   = useState([0, 0, 0, 0]);
  const [nodeCount, setNodeCount] = useState(0);
  const [events, setEvents] = useState([]);
  const [zoneFilter, setZoneFilter] = useState(null);
  const [overlay, setOverlay] = useState('none');   // none | generation | consumption
  const heatRef = useRef(null);
  const overlayRef = useRef('none');                // read inside the interval closure

  useEffect(() => {
    if (mapRef.current) return;

    const map = L.map(mapEl.current, { preferCanvas: true })
      .setView([18.54, 73.87], 12);
    mapRef.current = map;

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: 'Gridweaver',
      maxZoom: 19,
    }).addTo(map);

    const ZONE_CENTRES = {
      A: [18.54, 73.774], B: [18.54, 73.822], C: [18.54, 73.870],
      D: [18.54, 73.918], E: [18.54, 73.966],
    };
    ZONE_BOUNDS.forEach(({ zone, lngLo, lngHi }) => {
      L.rectangle([[LAT_LO, lngLo], [LAT_HI, lngHi]], {
        color: '#64748b', weight: 1, fillOpacity: 0.04, interactive: false,
      }).addTo(map);
      L.marker([LAT_HI + 0.008, (lngLo + lngHi) / 2], {
        icon: L.divIcon({ className: 'zone-label', html: zone }),
        interactive: false,
      }).addTo(map);
    });

    let socket = null;
    let retry = null;
    let dead = false;

    // Recount from the full array rather than tracking incrementally --
    // 10k array reads is trivial and avoids drift between snapshot and deltas.
    const recount = () => {
      const t = [0, 0, 0, 0];
      const s = statuses.current;
      for (let i = 0; i < s.length; i++) t[s[i]]++;
      setTally(t);
    };

    const applyDelta = (flat) => {
      const s = statuses.current;
      for (let i = 0; i < flat.length; i += 2) {
        const idx = flat[i], st = flat[i + 1];
        if (s[idx] === st) continue;
        s[idx] = st;
        markers.current[idx]?.setStyle({ fillColor: STATUS_COLORS[st] });
      }
    };

    const connect = () => {
      if (dead) return;
      socket = new WebSocket(WS);

      socket.onopen = () => setConn('live');

      socket.onmessage = (ev) => {
        const msg = JSON.parse(ev.data);
        if (msg.type === 'snapshot') {
          console.log('snapshot s:', msg.s?.length, msg.s?.slice(0, 5));
          statuses.current = Int8Array.from(msg.s);
          for (let i = 0; i < msg.s.length; i++) {
            markers.current[i]?.setStyle({ fillColor: STATUS_COLORS[msg.s[i]] });
          }
        } else if (msg.type === 'delta' && statuses.current) {
          applyDelta(msg.d);
        }
        if (msg.zones) setZones(msg.zones);
        if (statuses.current) recount();
        if (msg.events?.length) {
          setEvents(prev => [...msg.events.reverse(), ...prev].slice(0, 100));
        }
        if (msg.transfers) drawFlows(msg.transfers);
      };

      socket.onclose = () => {
        setConn('reconnecting');
        retry = setTimeout(connect, 1500);
      };
      socket.onerror = () => socket.close();
    };

    fetch(`${API}/api/topology`)
      .then(r => r.json())
      .then(rows => {
        if (dead) return;
        const layer = L.layerGroup();
        markers.current = new Array(rows.length);
        statuses.current = new Int8Array(rows.length);

        rows.forEach(([id, lat, lng, type], i) => {
          const m = L.circleMarker([lat, lng], {
            radius: 3, stroke: false,
            fillColor: STATUS_COLORS[0], fillOpacity: 0.8,
          }).bindTooltip(`${id}<br>${TYPE_NAMES[type]}`, { direction: 'top' });
          markers.current[i] = m;
          m.addTo(layer);
        });

        layer.addTo(map);
        setNodeCount(rows.length);
        connect();
      })
      .catch(err => {
        console.error('topology fetch failed:', err);
        setConn('topology failed');
      });

    return () => {
      dead = true;
      clearTimeout(retry);
      socket?.close();
      map.remove();
      mapRef.current = null;
    };
  },[]);

    // Heatmap overlay. Polls only while switched on; the delta stream carries
  // status, not power, so this is the only source of per-node magnitude.
  useEffect(() => {
    overlayRef.current = overlay;
    const map = mapRef.current;
    if (!map) return;

    if (overlay === 'none') {
      if (heatRef.current) {
        map.removeLayer(heatRef.current);
        heatRef.current = null;
      }
      return;
    }

    let cancelled = false;

    const gradient = overlay === 'generation'
      ? { 0.2: '#1e3a8a', 0.5: '#f5b301', 0.8: '#f97316', 1.0: '#fef08a' }
      : { 0.2: '#1e3a8a', 0.5: '#7c3aed', 0.8: '#db2777', 1.0: '#fda4af' };

    const poll = async () => {
      try {
        const res = await fetch(`${API}/api/heatmap?mode=${overlay}`);
        const flat = await res.json();
        if (cancelled || overlayRef.current !== overlay) return;

        const points = [];
        for (let i = 0; i < flat.length; i += 3) {
          points.push([flat[i], flat[i + 1], flat[i + 2]]);
        }

        if (!heatRef.current) {
          heatRef.current = L.heatLayer(points, {
            radius: 25,
            blur: 30,
            maxZoom: 14,
            max: 8,
            minOpacity: 0.35,
            gradient,
          }).addTo(map);
        } else {
          heatRef.current.setOptions({ gradient, max: 5, minOpacity: 0.35 });
          heatRef.current.setLatLngs(points);
        }
      } catch (e) {
        console.error('heatmap poll failed:', e);
      }
    };
        // Flow arrows between zone centres. Redrawn wholesale each tick rather
    // than diffed -- at most a handful of routes, so the churn is trivial and
    // the code stays obvious.
    const drawFlows = (transfers) => {
      if (flowRef.current) map.removeLayer(flowRef.current);
      if (!transfers.length) { flowRef.current = null; return; }

      const layer = L.layerGroup();
      const maxKw = Math.max(...transfers.map(t => t.kw), 1);

      transfers.forEach(({ from, to, kw }) => {
        const a = ZONE_CENTRES[from], b = ZONE_CENTRES[to];
        if (!a || !b) return;

        // Width encodes magnitude; the eye reads thickness faster than labels.
        const weight = 2 + (kw / maxKw) * 8;
        L.polyline([a, b], {
          color: '#38bdf8', weight, opacity: 0.75, dashArray: '10 6',
        }).addTo(layer);

        const mid = [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2];
        L.marker(mid, {
          icon: L.divIcon({
            className: 'flow-label',
            html: `${from}→${to} ${Math.round(kw)} kW`,
          }),
          interactive: false,
        }).addTo(layer);
      });

      layer.addTo(map);
      flowRef.current = layer;
    };

    poll();
    // 2s, not the 250ms tick: this is a ~250KB payload and the geographic
    // picture does not change meaningfully faster than that.
    const timer = setInterval(poll, 2000);

    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [overlay]);
  
  return (
    <div className="shell">
      <div ref={mapEl} className="map" />
      <aside className="sidebar">
        <h1>GridWeaver</h1>
        <p className="status">
          <span className={`dot ${conn}`} />
          {conn} · {nodeCount.toLocaleString()} nodes
        </p>

        <h2>States</h2>
        <ul className="legend">
          {STATUS_NAMES.map((name, i) => (
            <li key={name}>
              <span className="swatch" style={{ background: STATUS_COLORS[i] }} />
              {name}
              <span className="count">{tally[i].toLocaleString()}</span>
            </li>
          ))}
        </ul>

        <h2>Zones</h2>
        <table className="zones">
          <thead>
            <tr><th>Z</th><th>Gen</th><th>Con</th><th>LF</th><th>SoC</th></tr>
          </thead>
          <tbody>
            {zones.map(z => (
              <tr key={z.z}>
                <td>{z.z}</td>
                <td>{Math.round(z.gen)}</td>
                <td>{Math.round(z.con)}</td>
                <td>{z.lf.toFixed(2)}</td>
                <td>{z.soc.toFixed(2)}</td>
              </tr>
            ))}
          </tbody>
        </table>
                <h2>
          Event log
          {zoneFilter && (
            <button className="clear" onClick={() => setZoneFilter(null)}>
              {zoneFilter} ×
            </button>
          )}
        </h2>
        <div className="events">
          {events.length === 0 && <p className="empty">No transitions yet</p>}
          {events
            .filter(e => !zoneFilter || e.zone === zoneFilter || e.source === zoneFilter)
            .map(e => (
              <div key={e.seq} className="event">
                <div className="event-head">
                  <button className="zone-tag" onClick={() => setZoneFilter(e.zone)}>
                    {e.kind === 'ZONE_TRANSITION' ? e.zone : `${e.source}→${e.zone}`}
                  </button>
                  <span className={`to ${(e.to || e.kind).toLowerCase()}`}>
                    {e.kind === 'ZONE_TRANSITION' ? e.to : e.kind.replace('TRANSFER_', '')}
                  </span>
                  <span className="time">
                    {new Date(e.ts).toLocaleTimeString([], { hour12: false })}
                  </span>
                </div>
                <div className="event-detail">
                  {e.kind === 'ZONE_TRANSITION'
                    ? `${e.from} → ${e.to} · ${e.trigger.replace('_', ' ').toLowerCase()}`
                    : `${Math.round(e.amountKw)} kW`}
                </div>
              </div>
            ))}
        </div>
        <h2>Overlay</h2>
        <div className="overlay-buttons">
          {[
            ['none', 'Off'],
            ['generation', 'Generation'],
            ['consumption', 'Demand'],
          ].map(([key, label]) => (
            <button
              key={key}
              className={overlay === key ? 'ov active' : 'ov'}
              onClick={() => setOverlay(key)}
            >
              {label}
            </button>
          ))}
        </div>
        {overlay !== 'none' && (
          <div className="scale">
            <span className={`bar ${overlay}`} />
            <span className="scale-labels">
              <span>low</span><span>high kW</span>
            </span>
          </div>
        )}
      </aside>
    </div>
  );
}