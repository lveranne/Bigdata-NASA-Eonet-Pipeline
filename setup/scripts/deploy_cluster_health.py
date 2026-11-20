import json
import urllib.request
import urllib.error

GRAFANA_URL = "http://worker:3000"
HEADERS = {"Content-Type": "application/json"}

# 1. Detect or register Prometheus datasource in Grafana
req_ds = urllib.request.Request(f"{GRAFANA_URL}/api/datasources", headers=HEADERS)
with urllib.request.urlopen(req_ds) as resp:
    datasources = json.loads(resp.read().decode("utf-8"))

prom_ds = next((d for d in datasources if d.get("type") == "prometheus"), None)

if prom_ds is None:
    # Probe Prometheus on worker (localhost from Grafana's perspective) or master
    prom_url = "http://localhost:9090"
    for candidate in ["http://192.168.56.21:9090", "http://192.168.56.20:9090"]:
        try:
            with urllib.request.urlopen(f"{candidate}/api/v1/status/runtimeinfo", timeout=2) as r:
                if r.status == 200:
                    prom_url = "http://localhost:9090" if "56.21" in candidate else candidate
                    break
        except Exception:
            continue

    create_payload = {
        "name": "Prometheus-EONET",
        "type": "prometheus",
        "access": "proxy",
        "url": prom_url,
        "jsonData": {"httpMethod": "POST", "timeInterval": "15s"},
        "isDefault": False
    }
    req_create = urllib.request.Request(
        f"{GRAFANA_URL}/api/datasources",
        data=json.dumps(create_payload).encode("utf-8"),
        headers=HEADERS,
        method="POST"
    )
    with urllib.request.urlopen(req_create) as resp:
        created = json.loads(resp.read().decode("utf-8"))
        prom_uid = created["datasource"]["uid"]
else:
    prom_uid = prom_ds["uid"]

DS_REF = {"type": "prometheus", "uid": prom_uid}

def prom_target(expr: str, legend: str = "{{instance}}", format_type: str = "time_series", instant: bool = False) -> list:
    return [{
        "datasource": DS_REF,
        "editorMode": "code",
        "expr": expr,
        "format": format_type,
        "instant": instant,
        "legendFormat": legend,
        "range": not instant,
        "refId": "A"
    }]

STD_THRESHOLDS = {
    "mode": "absolute",
    "steps": [
        {"color": "green", "value": None},
        {"color": "orange", "value": 70},
        {"color": "red", "value": 85}
    ]
}

dashboard = {
    "id": None,
    "uid": "eonet-cluster-health",
    "title": "4. NASA EONET — Cluster Health (CPU, Disk & RAM)",
    "editable": True,
    "style": "dark",
    "tags": ["nasa-eonet", "infrastructure"],
    "timezone": "utc",
    "schemaVersion": 39,
    "refresh": "",
    "time": {"from": "now-1h", "to": "now"},
    "templating": {"list": []},
    "timepicker": {},
    "links": [],
    "annotations": {
        "list": [{
            "builtIn": 1,
            "datasource": {"type": "grafana", "uid": "-- Grafana --"},
            "enable": True,
            "hide": True,
            "iconColor": "rgba(0, 211, 255, 1)",
            "name": "Annotations & Alerts",
            "type": "dashboard"
        }]
    },
    "panels": [
        # ROW 1: Cluster Summary Gauges & Active Node Count (y=0, h=6)
        {
            "id": 1,
            "type": "stat",
            "title": "Online Cluster Nodes",
            "description": "Number of VMs actively reporting Node Exporter metrics.",
            "datasource": DS_REF,
            "gridPos": {"x": 0, "y": 0, "w": 6, "h": 6},
            "targets": prom_target("count(node_uname_info)", legend="Nodes", instant=True),
            "options": {
                "colorMode": "background",
                "graphMode": "none",
                "justifyMode": "center",
                "textMode": "value"
            },
            "fieldConfig": {
                "defaults": {
                    "unit": "short",
                    "decimals": 0,
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "red", "value": None},
                            {"color": "orange", "value": 1},
                            {"color": "green", "value": 2}
                        ]
                    }
                },
                "overrides": []
            }
        },
        {
            "id": 2,
            "type": "gauge",
            "title": "Cluster Avg CPU Usage",
            "datasource": DS_REF,
            "gridPos": {"x": 6, "y": 0, "w": 6, "h": 6},
            "targets": prom_target('100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[2m])) * 100)', legend="CPU %", instant=True),
            "options": {"showThresholdLabels": False, "showThresholdMarkers": True},
            "fieldConfig": {
                "defaults": {"unit": "percent", "min": 0, "max": 100, "decimals": 1, "thresholds": STD_THRESHOLDS},
                "overrides": []
            }
        },
        {
            "id": 3,
            "type": "gauge",
            "title": "Cluster Avg RAM Usage",
            "datasource": DS_REF,
            "gridPos": {"x": 12, "y": 0, "w": 6, "h": 6},
            "targets": prom_target("100 * (1 - sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes))", legend="RAM %", instant=True),
            "options": {"showThresholdLabels": False, "showThresholdMarkers": True},
            "fieldConfig": {
                "defaults": {"unit": "percent", "min": 0, "max": 100, "decimals": 1, "thresholds": STD_THRESHOLDS},
                "overrides": []
            }
        },
        {
            "id": 4,
            "type": "gauge",
            "title": "Cluster Avg Root Disk Usage",
            "datasource": DS_REF,
            "gridPos": {"x": 18, "y": 0, "w": 6, "h": 6},
            "targets": prom_target('100 * (1 - sum(node_filesystem_avail_bytes{mountpoint="/",fstype!="rootfs"}) / sum(node_filesystem_size_bytes{mountpoint="/",fstype!="rootfs"}))', legend="Disk %", instant=True),
            "options": {"showThresholdLabels": False, "showThresholdMarkers": True},
            "fieldConfig": {
                "defaults": {"unit": "percent", "min": 0, "max": 100, "decimals": 1, "thresholds": STD_THRESHOLDS},
                "overrides": []
            }
        },

        # ROW 2: CPU & RAM Time-Series per Node (y=6, h=8)
        {
            "id": 5,
            "type": "timeseries",
            "title": "CPU Utilization by Node (%)",
            "datasource": DS_REF,
            "gridPos": {"x": 0, "y": 6, "w": 12, "h": 8},
            "targets": prom_target('100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[2m])) * 100)', legend="{{instance}}"),
            "options": {
                "legend": {"displayMode": "table", "placement": "bottom", "calcs": ["mean", "max", "lastNotNull"]},
                "tooltip": {"mode": "multi", "sort": "desc"}
            },
            "fieldConfig": {
                "defaults": {
                    "unit": "percent",
                    "min": 0,
                    "max": 100,
                    "decimals": 1,
                    "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 15, "spanNulls": True}
                },
                "overrides": []
            }
        },
        {
            "id": 6,
            "type": "timeseries",
            "title": "RAM Utilization by Node (%)",
            "datasource": DS_REF,
            "gridPos": {"x": 12, "y": 6, "w": 12, "h": 8},
            "targets": prom_target("100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))", legend="{{instance}}"),
            "options": {
                "legend": {"displayMode": "table", "placement": "bottom", "calcs": ["mean", "max", "lastNotNull"]},
                "tooltip": {"mode": "multi", "sort": "desc"}
            },
            "fieldConfig": {
                "defaults": {
                    "unit": "percent",
                    "min": 0,
                    "max": 100,
                    "decimals": 1,
                    "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 15, "spanNulls": True}
                },
                "overrides": []
            }
        },

        # ROW 3: Hard Disk Usage & Disk I/O by Node (y=14, h=8)
        {
            "id": 7,
            "type": "bargauge",
            "title": "Root Hard Disk Usage by Node (%)",
            "datasource": DS_REF,
            "gridPos": {"x": 0, "y": 14, "w": 12, "h": 8},
            "targets": prom_target('100 * (1 - (node_filesystem_avail_bytes{mountpoint="/",fstype!="rootfs"} / node_filesystem_size_bytes{mountpoint="/",fstype!="rootfs"}))', legend="{{instance}}", instant=True),
            "options": {
                "orientation": "horizontal",
                "displayMode": "lcd",
                "valueMode": "color",
                "showUnfilled": True
            },
            "fieldConfig": {
                "defaults": {"unit": "percent", "min": 0, "max": 100, "decimals": 1, "thresholds": STD_THRESHOLDS},
                "overrides": []
            }
        },
        {
            "id": 8,
            "type": "timeseries",
            "title": "Disk I/O Throughput by Node (MB/s)",
            "datasource": DS_REF,
            "gridPos": {"x": 12, "y": 14, "w": 12, "h": 8},
            "targets": prom_target("sum by (instance) (rate(node_disk_read_bytes_total[2m]) + rate(node_disk_written_bytes_total[2m])) / 1048576", legend="{{instance}}"),
            "options": {
                "legend": {"displayMode": "table", "placement": "bottom", "calcs": ["mean", "max", "lastNotNull"]},
                "tooltip": {"mode": "multi", "sort": "desc"}
            },
            "fieldConfig": {
                "defaults": {
                    "unit": "MBs",
                    "min": 0,
                    "decimals": 2,
                    "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 15, "spanNulls": True}
                },
                "overrides": []
            }
        }
    ]
}

with open("setup/grafana/eonet-cluster-health.json", "w", encoding="utf-8") as f:
    json.dump(dashboard, f, indent=2)

req_post = urllib.request.Request(
    f"{GRAFANA_URL}/api/dashboards/db",
    data=json.dumps({"dashboard": dashboard, "overwrite": True}).encode("utf-8"),
    headers=HEADERS,
    method="POST"
)
with urllib.request.urlopen(req_post) as resp:
    print("Deployed Cluster Health dashboard:", resp.read().decode("utf-8"))
