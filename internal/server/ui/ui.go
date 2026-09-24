package ui

import (
	"html/template"
	"log/slog"
	"net/http"

	"github.com/gke-labs/extensible-workload-autoscaler/internal/server/store"
)

// RegisterHandlers registers the UI handlers on the provided mux.
func RegisterHandlers(mux *http.ServeMux, s store.MetricStore) {
	mux.HandleFunc("/", handleIndex(s))
}

func handleIndex(s store.MetricStore) http.HandlerFunc {
	tmpl, err := template.New("index").Parse(pageTemplate)

	if err != nil {
		slog.Error("Failed to parse UI template", "error", err)
		panic(err)
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}

		if err := tmpl.Execute(w, nil); err != nil {
			slog.Error("Failed to render UI", "error", err)
		}
	}
}

const pageTemplate = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>XAS Control Plane</title>
    <style>
        :root {
            --primary: #2563eb;
            --bg: #f8fafc;
            --surface: #ffffff;
            --text: #1e293b;
            --border: #e2e8f0;
            --success: #10b981;
            --warning: #f59e0b;
            --danger: #ef4444;
            --muted: #64748b;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background-color: var(--bg);
            color: var(--text);
            margin: 0;
            padding: 20px;
            line-height: 1.5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 30px;
            padding-bottom: 20px;
            border-bottom: 1px solid var(--border);
        }
        h1 { margin: 0; font-size: 1.5rem; }
        .policy-list {
            display: flex;
            flex-direction: column;
            gap: 20px;
        }
        details.policy-card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        }
        details.policy-card summary {
            padding: 15px 20px;
            cursor: pointer;
            list-style: none; /* Hide default marker */
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: var(--surface);
            font-weight: 500;
        }
        details.policy-card summary::-webkit-details-marker {
            display: none;
        }
        details.policy-card[open] summary {
            border-bottom: 1px solid var(--border);
            background: #f1f5f9;
        }
        .summary-left {
            display: flex;
            flex-direction: column;
        }
        .summary-right {
            display: flex;
            gap: 20px;
            align-items: center;
        }
        .policy-name {
            font-weight: 600;
            font-size: 1.1rem;
            color: var(--primary);
        }
        .namespace {
            font-size: 0.85rem;
            color: var(--muted);
        }
        .workload-info {
            font-size: 0.75rem;
            color: var(--muted);
            margin-top: 2px;
        }
        .text-muted-small {
            font-size: 0.85em;
            color: var(--muted);
        }
        .stat-badge {
            background: #e2e8f0;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.8rem;
            display: flex;
            flex-direction: column;
            align-items: center;
            min-width: 60px;
        }
        .stat-badge span:first-child { font-weight: 700; font-size: 1rem; }
        .stat-badge span:last-child { font-size: 0.65rem; color: var(--muted); text-transform: uppercase; }
        
        .details-content {
            padding: 20px;
        }
        .section {
            margin-bottom: 25px;
        }
        .section-title {
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--muted);
            margin-bottom: 10px;
            font-weight: 600;
            border-bottom: 1px solid var(--border);
            padding-bottom: 5px;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }
        th, td {
            text-align: left;
            padding: 8px 12px;
            border-bottom: 1px solid var(--border);
        }
        th {
            font-weight: 600;
            color: var(--muted);
            background: #f8fafc;
        }
        tr:last-child td { border-bottom: none; }

        .tag {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 600;
        }
        .tag-active { background: #dcfce7; color: #166534; }
        .tag-inactive { background: #f1f5f9; color: var(--muted); }
        .tag-ready { background: #dcfce7; color: #166534; }
        .tag-not-ready { background: #fee2e2; color: #991b1b; }
        .tag-owner { background: #e0e7ff; color: #3730a3; }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>XAS Control Plane</h1>
            <div>
                <a href="/storez" style="color: var(--primary); text-decoration: none; font-size: 0.9rem;">View Raw JSON</a>
            </div>
        </header>

        <div id="policy-list" class="policy-list">
            <!-- Content will be rendered by JavaScript -->
            <div style="text-align: center; color: var(--muted); padding: 40px;">Loading...</div>
        </div>
    </div>
    <script>
        const policyList = document.getElementById('policy-list');
        const POLL_INTERVAL = 2000;

        async function updateDashboard() {
            try {
                const response = await fetch('/storez');
                if (!response.ok) {
                    throw new Error('Network response was not ok');
                }
                const rawData = await response.json();
                renderPolicies(rawData);
            } catch (error) {
                console.error('Failed to fetch data:', error);
            }
        }

        function renderPolicies(data) {
            // Convert object to array and sort
            const policies = Object.values(data)
                .filter(ps => ps && ps.Policy && ps.Policy.id)
                .sort((a, b) => {
                if (a.Policy.id.namespace !== b.Policy.id.namespace) {
                    return a.Policy.id.namespace.localeCompare(b.Policy.id.namespace);
                }
                return a.Policy.id.name.localeCompare(b.Policy.id.name);
            });

            if (policies.length === 0) {
                policyList.innerHTML = '<div style="text-align: center; padding: 40px; color: var(--muted);">No ScalingPolicies found.</div>';
                return;
            }

            const currentIds = new Set();

            policies.forEach(ps => {
                const id = "policy-" + ps.Policy.id.namespace + "-" + ps.Policy.id.name;
                currentIds.add(id);

                let card = document.getElementById(id);
                if (!card) {
                    card = document.createElement('details');
                    card.className = 'policy-card';
                    card.id = id;
                    card.open = true; // Default open
                    policyList.appendChild(card);
                }

                updatePolicyCard(card, ps);
            });

            // Cleanup removed policies
            Array.from(policyList.children).forEach(child => {
                if (child.id && !currentIds.has(child.id)) {
                    policyList.removeChild(child);
                }
            });
            
            // Remove initial loading message if present
            if (policyList.children.length > 0 && policyList.children[0].innerText === 'Loading...') {
                 policyList.removeChild(policyList.children[0]);
            }
        }

        function updatePolicyCard(card, ps) {
            const currentReplicas = ps.Workload ? Object.keys(ps.Workload).length : 0;
            const targetReplicas = ps.Recommendation ? ps.Recommendation.replicas : '-';
            const maxReplicas = ps.Policy.max_replicas;

            // Summary
            const summaryHTML = 
                '<div class="summary-left">' +
                    '<span class="policy-name">' + ps.Policy.id.name + '</span>' +
                    '<span class="namespace">' + ps.Policy.id.namespace + '</span>' +
                    '<span class="workload-info">' + ps.Policy.workload.kind + ': ' + ps.Policy.workload.name + '</span>' +
                '</div>' +
                '<div class="summary-right">' +
                    '<div class="stat-badge">' +
                        '<span>' + currentReplicas + '</span>' +
                        '<span>Current</span>' +
                    '</div>' +
                    '<div class="stat-badge">' +
                        '<span>' + targetReplicas + '</span>' +
                        '<span>Target</span>' +
                    '</div>' +
                     '<div class="stat-badge">' +
                        '<span>' + maxReplicas + '</span>' +
                        '<span>Max</span>' +
                    '</div>' +
                '</div>';
            
            let summary = card.querySelector('summary');
            if (!summary) {
                summary = document.createElement('summary');
                card.appendChild(summary);
            }
            if (summary.innerHTML !== summaryHTML) {
                 summary.innerHTML = summaryHTML;
            }

            // Content
            let content = card.querySelector('.details-content');
            if (!content) {
                content = document.createElement('div');
                content.className = 'details-content';
                card.appendChild(content);
            }

            const metricsHTML = renderMetricsTable(ps);
            const decisionsHTML = renderDecisionsTable(ps);
            const workloadHTML = renderWorkloadTable(ps);

            const newContentHTML = metricsHTML + decisionsHTML + workloadHTML;
            if (content.innerHTML !== newContentHTML) {
                content.innerHTML = newContentHTML;
            }
        }

        // metricOwners returns every scope holding control metrics: the
        // policy-wide one first, then one per recommender owning metrics.
        // Metric names are only unique within their owner.
        function metricOwners(ps) {
            const owners = [{
                name: '',
                defs: ps.Policy.metrics || [],
                cm: ps.ControlMetrics,
            }];
            const owned = ps.Policy.recommender_metrics || {};
            Object.keys(owned).sort().forEach(name => {
                owners.push({
                    name: name,
                    defs: (owned[name] && owned[name].definitions) || [],
                    cm: (ps.RecommenderControlMetrics || {})[name],
                });
            });
            return owners;
        }

        function ownerCell(owner) {
            if (!owner) return '<span class="text-muted-small">Policy</span>';
            return '<span class="tag tag-owner">' + owner + '</span>';
        }

        // describeMetric extracts the intent of a metric definition for display.
        function describeMetric(def, defaultScope) {
            def = def || {};
            const d = {
                intent: '-',
                agg: '-',
                config: '',
                scope: def.scope || defaultScope,
                params: '',
            };

            if (def.gauge) {
                d.intent = 'Gauge';
                d.agg = def.gauge.aggregation || 'Avg';
            } else if (def.rate) {
                d.intent = 'Rate';
                d.agg = def.rate.aggregation || 'Sum';
                d.config = 'Window: ' + def.rate.window;
            } else if (def.distribution) {
                d.intent = 'Distribution';
                d.agg = def.distribution.percentile;
                d.config = 'Agg: ' + (def.distribution.aggregation || 'Max');
            } else if (def.decaying_distribution) {
                d.intent = 'DecayingDist';
                d.agg = def.decaying_distribution.percentile;
                d.config = 'HL: ' + def.decaying_distribution.half_life;
            }

            if (def.params) {
                d.params = Object.entries(def.params).map(([k, v]) => k + '=' + v).join(', ');
            }
            return d;
        }

        // renderMetricRows renders the control metrics of a single owner.
        function renderMetricRows(owner) {
            const cm = owner.cm;
            if (!cm) return '';

            const metricDefs = {};
            owner.defs.forEach(m => metricDefs[m.name] = m);

            let rows = '';

            // Render Global/Aggregated Metrics
            if (cm.values) {
                for (const [name, val] of Object.entries(cm.values)) {
                    const d = describeMetric(metricDefs[name], 'Global');

                    rows += '<tr>' +
                            '<td>' + name + ' <span class="tag tag-inactive">' + d.scope + '</span></td>' +
                            '<td>' + ownerCell(owner.name) + '</td>' +
                            '<td class="text-muted-small">' + d.intent + '</td>' +
                            '<td class="text-muted-small">' + d.agg + '</td>' +
                            '<td class="text-muted-small">' + d.params + '</td>' +
                            '<td class="text-muted-small">' + d.config + '</td>' +
                            '<td><strong>' + formatFloat(val) + '</strong></td>' +
                            '<td>' + formatTime(cm.timestamp) + '</td>' +
                        '</tr>';
                }
            }

            // Render Pod- and Container-Scoped Metrics (as distinct rows for clarity or grouped)
            if (cm.pod_metrics && Object.keys(cm.pod_metrics).length > 0) {
                // Collect all pod metrics by metric name first to keep the table organized
                const podMetricsByName = {};
                const podContainerMetrics = cm.pod_container_metrics || {};
                for (const [podName, podData] of Object.entries(cm.pod_metrics)) {
                    const podValues = (podData && podData.values) || {};
                    const containerMetrics = (podContainerMetrics[podName] && podContainerMetrics[podName].container_metrics) || {};

                    for (const [name, val] of Object.entries(podValues)) {
                        if (!podMetricsByName[name]) podMetricsByName[name] = [];

                        // Collect the per-container breakdown for this metric, if any.
                        const containers = [];
                        for (const [containerName, containerData] of Object.entries(containerMetrics)) {
                            const containerValues = (containerData && containerData.values) || {};
                            if (name in containerValues) {
                                containers.push({container: containerName, val: containerValues[name]});
                            }
                        }
                        containers.sort((a, b) => a.container.localeCompare(b.container));

                        podMetricsByName[name].push({pod: podName, val: val, containers: containers});
                    }
                }

                for (const [name, pods] of Object.entries(podMetricsByName)) {
                    const d = describeMetric(metricDefs[name], 'Pod');

                    // Build a string displaying the pod values, with the
                    // per-container breakdown nested underneath when available.
                    let podValsHtml = '<div style="max-height: 80px; overflow-y: auto; font-size: 0.9rem;">';
                    pods.forEach(p => {
                         podValsHtml += '<div><span class="text-muted-small">' + p.pod + ':</span> <strong>' + formatFloat(p.val) + '</strong></div>';
                         (p.containers || []).forEach(c => {
                             podValsHtml += '<div style="padding-left: 1rem;"><span class="text-muted-small">&#8627; ' + c.container + ':</span> ' + formatFloat(c.val) + '</div>';
                         });
                    });
                    podValsHtml += '</div>';

                    rows += '<tr>' +
                            '<td>' + name + ' <span class="tag tag-inactive">' + d.scope + '</span></td>' +
                            '<td>' + ownerCell(owner.name) + '</td>' +
                            '<td class="text-muted-small">' + d.intent + '</td>' +
                            '<td class="text-muted-small">' + d.agg + '</td>' +
                            '<td class="text-muted-small">' + d.params + '</td>' +
                            '<td class="text-muted-small">' + d.config + '</td>' +
                            '<td>' + podValsHtml + '</td>' +
                            '<td>' + formatTime(cm.timestamp) + '</td>' +
                        '</tr>';
                }
            }

            // Render Container-Scoped Metrics: one value per container name,
            // averaged over the pods reporting it.
            const workloadContainers = (cm.container_metrics && cm.container_metrics.container_metrics) || {};
            if (Object.keys(workloadContainers).length > 0) {
                // Group by metric name so each metric gets a single row.
                const containerMetricsByName = {};
                for (const [containerName, containerData] of Object.entries(workloadContainers)) {
                    const containerValues = (containerData && containerData.values) || {};
                    for (const [name, val] of Object.entries(containerValues)) {
                        if (!containerMetricsByName[name]) containerMetricsByName[name] = [];
                        containerMetricsByName[name].push({container: containerName, val: val});
                    }
                }

                for (const [name, containers] of Object.entries(containerMetricsByName)) {
                    const d = describeMetric(metricDefs[name], 'Container');
                    containers.sort((a, b) => a.container.localeCompare(b.container));

                    let containerValsHtml = '<div style="max-height: 80px; overflow-y: auto; font-size: 0.9rem;">';
                    containers.forEach(c => {
                        containerValsHtml += '<div><span class="text-muted-small">' + c.container + ':</span> <strong>' + formatFloat(c.val) + '</strong></div>';
                    });
                    containerValsHtml += '</div>';

                    rows += '<tr>' +
                            '<td>' + name + ' <span class="tag tag-inactive">' + d.scope + '</span></td>' +
                            '<td>' + ownerCell(owner.name) + '</td>' +
                            '<td class="text-muted-small">' + d.intent + '</td>' +
                            '<td class="text-muted-small">' + d.agg + '</td>' +
                            '<td class="text-muted-small">' + d.params + '</td>' +
                            '<td class="text-muted-small">' + d.config + '</td>' +
                            '<td>' + containerValsHtml + '</td>' +
                            '<td>' + formatTime(cm.timestamp) + '</td>' +
                        '</tr>';
                }
            }

            return rows;
        }

        function renderMetricsTable(ps) {
            // Policy-wide metrics and metrics owned by a recommender are all
            // reported here, told apart by the Owner column.
            let rows = '';
            metricOwners(ps).forEach(owner => { rows += renderMetricRows(owner); });

            if (!rows) {
                rows = '<tr><td colspan="8" style="text-align: center; color: var(--muted);">No metrics collected yet.</td></tr>';
            }

            return '<div class="section">' +
                    '<div class="section-title">Control Metrics</div>' +
                    '<table>' +
                        '<thead>' +
                            '<tr>' +
                                '<th>Metric Name</th>' +
                                '<th>Owner</th>' +
                                '<th>Intent</th>' +
                                '<th>Resolution</th>' +
                                '<th>Params</th>' +
                                '<th>Config</th>' +
                                '<th>Value</th>' +
                                '<th>Last Updated</th>' +
                            '</tr>' +
                        '</thead>' +
                        '<tbody>' + rows + '</tbody>' +
                    '</table>' +
                '</div>';
        }

        function renderDecisionsTable(ps) {
            let rows = '';
            
            // Build lookup for config
            const recConfig = {};
            if (ps.Policy.scaling) ps.Policy.scaling.forEach(r => recConfig[r.name] = r.params);
            if (ps.Policy.activation) ps.Policy.activation.forEach(r => recConfig[r.name] = r.params);

            if (ps.Decisions) {
                // Sort decisions by name for stability
                const sortedDecisions = Object.values(ps.Decisions).sort((a, b) => a.name.localeCompare(b.name));
                
                for (const d of sortedDecisions) {
                     const activeClass = d.is_active ? 'tag-active' : 'tag-inactive';
                     const activeText = d.is_active ? 'Active' : 'Inactive';
                     
                     let paramsStr = '';
                     const params = recConfig[d.name];
                     if (params) {
                         paramsStr = Object.entries(params)
                            .map(([k, v]) => k + '=' + v)
                            .join(', ');
                     }

                     let voteHtml = '';
                     if (d.replicas !== undefined && d.replicas !== null) {
                         voteHtml += '<div><strong>Rep:</strong> ' + d.replicas + '</div>';
                     }

                     if (Array.isArray(d.workload_resources)) {
                         for (const wr of d.workload_resources) {
                             if (!wr || (!wr.requests && !wr.limits)) {
                                 continue;
                             }
                             let resHtml = '<div style="margin-top: 4px; font-size: 0.9rem; color: var(--primary);">';
                             if (wr.container_name) {
                                 resHtml += '<strong>Container:</strong> ' + wr.container_name + '<br>';
                             }
                             if (wr.requests) {
                                 resHtml += '<strong>Req:</strong> ' + Object.entries(wr.requests).map(([k,v])=>k+':'+v).join(', ') + '<br>';
                             }
                             if (wr.limits) {
                                 resHtml += '<strong>Lim:</strong> ' + Object.entries(wr.limits).map(([k,v])=>k+':'+v).join(', ');
                             }
                             resHtml += '</div>';
                             voteHtml += resHtml;
                         }
                     }

                     if (d.pod_resources && d.pod_resources.length > 0) {
                         voteHtml += '<div style="margin-top: 4px; font-size: 0.9rem; color: var(--primary);"><strong>Pods:</strong> ' + d.pod_resources.length + ' targeted updates</div>';
                     }

                     if (!voteHtml) {
                         voteHtml = '<span class="text-muted-small">-</span>';
                     }

                     rows += '<tr>' +
                            '<td>' + d.name + '</td>' +
                            '<td class="text-muted-small">' + d.type + '</td>' +
                            '<td class="text-muted-small">' + paramsStr + '</td>' +
                            '<td>' + voteHtml + '</td>' +
                            '<td><span class="tag ' + activeClass + '">' + activeText + '</span></td>' +
                            '<td class="text-muted-small">' + (d.message || '') + '</td>' +
                        '</tr>';
                }
            }
             if (!rows) {
                rows = '<tr><td colspan="6" style="text-align: center; color: var(--muted);">No decisions made yet.</td></tr>';
            }

            return '<div class="section">' +
                    '<div class="section-title">Recommender Decisions</div>' +
                    '<table>' +
                        '<thead>' +
                            '<tr>' +
                                '<th>Recommender</th>' +
                                '<th>Type</th>' +
                                '<th>Params</th>' +
                                '<th>Vote</th>' +
                                '<th>Status</th>' +
                                '<th>Message</th>' +
                            '</tr>' +
                        '</thead>' +
                        '<tbody>' + rows + '</tbody>' +
                    '</table>' +
                '</div>';
        }

        function renderWorkloadTable(ps) {
            const pods = ps.Workload ? Object.values(ps.Workload).sort((a, b) => a.name.localeCompare(b.name)) : [];

            // One column per metric, policy-wide ones and recommender-owned
            // ones alike. Series are keyed by <owner>/<name>, matching the
            // metric key used by the store.
            const metrics = [];
            metricOwners(ps).forEach(owner => {
                owner.defs.forEach(m => {
                    metrics.push({
                        name: m.name,
                        owner: owner.name,
                        key: owner.name ? owner.name + '/' + m.name : m.name,
                    });
                });
            });

            let headerCells = '';
            metrics.forEach(m => {
                headerCells += '<th>' + m.name +
                    (m.owner ? ' <span class="tag tag-owner">' + m.owner + '</span>' : '') +
                    '</th>';
            });

            let rows = '';
            if (pods.length > 0) {
                pods.forEach(pod => {
                    const statusClass = pod.is_ready ? 'tag-ready' : 'tag-not-ready';
                    const statusText = pod.is_ready ? 'Ready' : 'Not Ready';
                    
                    let metricCells = '';
                    metrics.forEach(m => {
                         let val = '-';
                         if (ps.Series && ps.Series[m.key]) {
                             for (const series of Object.values(ps.Series[m.key])) {
                                 // Note: Internal structs (Series) use Capitalized fields
                                 if (series.PodName === pod.name) {
                                     val = formatFloat(series.ControlMetric.Value);
                                     break;
                                 }
                             }
                         }
                         metricCells += '<td>' + val + '</td>';
                    });

                    rows += '<tr>' +
                            '<td>' + pod.name + '</td>' +
                            '<td><span style="font-family: monospace; font-size: 0.85em;">' + pod.node_name + '</span></td>' +
                            '<td><span class="tag ' + statusClass + '">' + statusText + '</span></td>' +
                            metricCells +
                        '</tr>';
                });
            } else {
                 const colspan = 3 + metrics.length;
                 rows = '<tr><td colspan="' + colspan + '" style="text-align: center; color: var(--muted);">No pods tracked.</td></tr>';
            }

            return '<div class="section">' +
                    '<div class="section-title">Workload Pods (' + pods.length + ')</div>' +
                    '<table>' +
                        '<thead>' +
                            '<tr>' +
                                '<th>Pod Name</th>' +
                                '<th>Node</th>' +
                                '<th>Status</th>' +
                                headerCells +
                            '</tr>' +
                        '</thead>' +
                        '<tbody>' + rows + '</tbody>' +
                    '</table>' +
                '</div>';
        }

        function formatTime(ts) {
            if (!ts) return '-';
            const date = new Date(ts * 1000);
            return date.toLocaleTimeString();
        }

        function formatFloat(val) {
             if (val === undefined || val === null) return '-';
             return parseFloat(val.toFixed(2));
        }

        updateDashboard();
        setInterval(updateDashboard, POLL_INTERVAL);
    </script>
</body>
</html>
`
