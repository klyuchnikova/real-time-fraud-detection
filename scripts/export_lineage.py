"""Export the actual project source/model lineage from dbt manifest as a static SVG."""
import html
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]

def main():
    manifest=json.loads((ROOT/"dbt/target/manifest.json").read_text(encoding="utf-8"))
    resources={**manifest["sources"],**manifest["nodes"]}
    nodes={key:value for key,value in resources.items()
           if value["package_name"]=="transaction_analytics" and value["resource_type"] in ("model","source")}
    levels={}
    while len(levels)<len(nodes):
        ready=[key for key,node in nodes.items() if key not in levels
               and all(parent in levels for parent in node.get("depends_on",{}).get("nodes",[]) if parent in nodes)]
        if not ready:
            raise ValueError("Cyclic model graph")
        for key in ready:
            parents=[parent for parent in nodes[key].get("depends_on",{}).get("nodes",[]) if parent in levels]
            levels[key]=0 if not parents else max(levels[parent] for parent in parents)+1
    positions={}
    width,height=1120,680
    node_width,node_height=280,66
    colors=["#eee9ff","#e3f2ff","#fff0d8"]
    for level in sorted(set(levels.values())):
        identifiers=sorted((key for key in nodes if levels[key]==level),key=lambda key:nodes[key]["name"])
        gap=34
        start=110+(500-(len(identifiers)*node_height+(len(identifiers)-1)*gap))/2
        for index,key in enumerate(identifiers):
            positions[key]=(35+level*375,start+index*(node_height+gap))
    parts=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
           '<rect width="100%" height="100%" fill="#ffffff"/>',
           '<defs><marker id="arrow" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto"><path d="M0,0 L0,6 L9,3 z" fill="#718096"/></marker></defs>',
           '<g font-family="Arial, sans-serif">',
           '<text x="35" y="42" font-size="24" font-weight="bold" fill="#172b4d">dbt lineage</text>',
           '<text x="35" y="68" font-size="13" fill="#526174">Original labeled transactions → staging → four analytical marts</text>']
    for level,label in enumerate(["RAW SOURCES","STAGING","MARTS"]):
        parts.append(f'<text x="{35+level*375}" y="108" font-size="13" font-weight="bold" fill="#526174">{label}</text>')
    edges=[]
    for key,node in nodes.items():
        for parent in node.get("depends_on",{}).get("nodes",[]):
            if parent not in nodes: continue
            edges.append((parent,key))
            x1,y1=positions[parent]
            x2,y2=positions[key]
            x1+=node_width; y1+=node_height/2; y2+=node_height/2
            middle=(x1+x2)/2
            parts.append(f'<path d="M{x1},{y1} C{middle},{y1} {middle},{y2} {x2},{y2}" fill="none" stroke="#718096" stroke-width="1.6" marker-end="url(#arrow)"/>')
    for key,node in nodes.items():
        x,y=positions[key]
        level=levels[key]
        label=("raw."+node["name"]) if node["resource_type"]=="source" else node["name"]
        kind="source" if node["resource_type"]=="source" else node["config"]["materialized"]
        parts.append(f'<rect x="{x}" y="{y}" width="{node_width}" height="{node_height}" rx="8" fill="{colors[min(level,2)]}" stroke="#bcc8d6"/>')
        parts.append(f'<text x="{x+14}" y="{y+28}" font-size="14" font-weight="bold" fill="#172b4d">{html.escape(label)}</text>')
        parts.append(f'<text x="{x+14}" y="{y+49}" font-size="12" fill="#526174">{html.escape(kind)}</text>')
    parts.extend(['<text x="35" y="650" font-size="12" fill="#526174">Exported directly from depends_on.nodes in manifest.json; package internals and data tests are omitted.</text>','</g></svg>'])
    output=ROOT/"docs/lineage.svg"
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text("\n".join(parts)+"\n",encoding="utf-8")
    print(json.dumps({"lineage_svg":str(output),"nodes":len(nodes),"edges":len(edges)}))

if __name__=="__main__":
    main()
