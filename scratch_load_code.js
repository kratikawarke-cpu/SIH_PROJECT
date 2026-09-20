function loadScenarios(){
1070:     try {
1071:       const data = await api('/api/scenarios?limit=100');
1072:       renderScenarioGrid(data.scenarios || []);
1073:     } catch(e) {
1074:       scenarioGrid.innerHTML = `<div class="panel" style="grid-column:1/-1;"><div class="empty-state"><div class="glyph">⚠</div><h3>Could not load scenarios</h3><p>${escapeHtml(e.message)}</p></div></div>`;
1075:     }
1076:   }
1077: 
1078:   function relTime(iso){
1079:     if (!iso) return '';
1080:     const d = new Date(iso);
1081:     if (isNaN(d)) return '';
1082:     const s = Math.floor((Date.now() - d.getTime()) / 1000);
1083:     if (s < 60) return s + 's ago';
1084:     if (s < 3600) return Math.floor(s/60) + 'm ago';
1085:     if (s < 86400) return Math.floor(s/3600) + 'h ago';
1086:     return Math.floor(s/86400) + 'd ago';
1087:   }
1088: 
1089:   function renderScenarioGrid(scenarios){
1090:     if (!scenarios.length) {
1091:       scenarioGrid.innerHTML = `<div class="panel" style="grid-column:1/-1;"><div class="empty-state"><div class="glyph">◌</div><h3>No scenarios yet</h3><p>Detected mule chains will appear here live as the detector finds them.</p></div></div>`;
1092:       return;
1093:     }
1094:     const isFirstLoad = !scenariosBooted;
1095:     scenarioGrid.innerHTML = '';
1096:     scenarios.forEach(s => {
1097:       const isNew = !isFirstLoad && !knownScenarioIds.has(s.alert_id);
1098:       const card = document.createElement('div');
1099:       card.className = 'scn-card' + (isNew ? ' is-new' : '');
1100:       const finals = (s.final_accounts || []).join(', ');
1101:       card.innerHTML = `
1102:         <div class="scn-top">
1103:           <span class="scn-id mono">${escapeHtml(s.alert_id)}</span>
1104:           <span class="scn-pattern pat-${escapeHtml(s.pattern||'')}">${escapeHtml(s.pattern||'—')}</span>
1105:         </div>
1106:         <div class="scn-flow">
1107:           <span class="src">${escapeHtml(s.source_account||'?')}</span><span class="arrow">→</span>
1108:           <span class="mul">${escapeHtml(s.mule_account||'?')}</span><span class="arrow">→</span>
1109:           <span class="fin">${escapeHtml(finals||'?')}</span>
1110:         </div>
1111:         <div class="scn-meta">
1112:           <span>${relTime(s.detected_at)}</span>
1113:           <span class="scn-states">${(s.states||[]).slice(0,3).map(st=>`<span class="state-tag">${escapeHtml(st)}</span>`).join('')}</span>
1114:         </div>`;
1115:       card.addEventListener('click', () => openScenarioModal(s.alert_id));
1116:       scenarioGrid.appendChild(card);
1117:       knownScenarioIds.add(s.alert_id);
1118:     });
1119:     scenariosBooted = true;
1120:   }
1121: 
1122:   setInterval(() => { if (drawerOpen) loadScenarios(); }, 5000);
1123: 
1124:   /* ===================== MODAL ===================== */
1125:   const modalVeil = document.getElementById('modalVeil');
1126:   const modalTitle = document.getElementById('modalTitle');
1127:   const modalSub = document.getElementById('modalSub');
1128:   const modalStats = document.getElementById('modalStats');
1129:   const modalGraph = document.getElementById('modalGraph');
1130:   const txTableWrap = document.getElementById('txTableWrap');
1131: 
1132:   // Modal Tab Elements
1133:   const tabGraph = document.getElementById('tabGraph');
1134:   const tabMap = document.getElementById('tabMap');
1135:   const tabLedger = document.getElementById('tabLedger');
1136:   const modalAtmCount = do