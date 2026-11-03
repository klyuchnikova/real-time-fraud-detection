"""Stream dbt output into a reviewable log and preserve structured test results."""
import argparse
import shutil
import subprocess
from pathlib import Path

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--log",type=Path,required=True)
    args,command=parser.parse_known_args()
    args.log.parent.mkdir(parents=True,exist_ok=True)
    process=subprocess.Popen(["dbt","--no-use-colors",*command,"--project-dir","dbt","--profiles-dir","dbt"],
                             stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,encoding="utf-8")
    with args.log.open("w",encoding="utf-8") as output:
        for line in process.stdout:
            print(line,end="",flush=True)
            output.write(line)
    status=process.wait()
    path=Path("dbt/target/run_results.json")
    if path.exists():
        shutil.copyfile(path,args.log.with_suffix(".json"))
    raise SystemExit(status)

if __name__=="__main__":
    main()
