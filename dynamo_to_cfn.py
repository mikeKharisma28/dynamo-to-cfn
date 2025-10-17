#!/usr/bin/env python3
"""
dynamo_describe_to_cfn_autoenc.py

Usage:
  python dynamo_describe_to_cfn_autoenc.py BalanceHistory.json -o balance-cfn.json
  python dynamo_describe_to_cfn_autoenc.py describes_dir/ --out-dir cfn_templates/

This script:
- Detects UTF-8 / UTF-16 (LE/BE) BOMs and decodes accordingly
- Converts aws dynamodb describe-table JSON -> CloudFormation template
- Writes output JSON (pretty printed)
"""
import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

def read_json_with_encoding(path: Path) -> Dict[str, Any]:
    raw = path.read_bytes()
    # BOM-aware decoding
    if raw.startswith(b'\xff\xfe') or raw.startswith(b'\xfe\xff'):
        text = raw.decode('utf-16')
    elif raw.startswith(b'\xef\xbb\xbf'):
        text = raw.decode('utf-8-sig')
    else:
        # try utf-8, fallback to utf-16
        try:
            text = raw.decode('utf-8')
        except UnicodeDecodeError:
            text = raw.decode('utf-16')
    return json.loads(text)

def make_logical_name(table_name: str) -> str:
    if not table_name:
        return "DynamoDBTable"
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in table_name)
    if not cleaned[0].isalpha():
        cleaned = "T_" + cleaned
    parts = [p for p in cleaned.split("_") if p]
    return "".join(p.capitalize() for p in parts)[:255]

def extract_table(describe: Dict[str, Any]) -> Dict[str, Any]:
    if "Table" in describe and isinstance(describe["Table"], dict):
        return describe["Table"]
    if "TableName" in describe and "KeySchema" in describe:
        return describe
    raise ValueError("Input JSON not recognized as describe-table output")

def convert_table_to_cfn(table: Dict[str, Any], logical_name: str) -> Dict[str, Any]:
    props: Dict[str, Any] = {
        "TableName": table.get("TableName"),
        "AttributeDefinitions": table.get("AttributeDefinitions", []),
        "KeySchema": table.get("KeySchema", []),
    }
    billing = table.get("BillingModeSummary", {}).get("BillingMode")
    if billing == "PAY_PER_REQUEST":
        props["BillingMode"] = "PAY_PER_REQUEST"
    else:
        pt = table.get("ProvisionedThroughput")
        if pt and "ReadCapacityUnits" in pt and "WriteCapacityUnits" in pt:
            props["ProvisionedThroughput"] = {
                "ReadCapacityUnits": pt["ReadCapacityUnits"],
                "WriteCapacityUnits": pt["WriteCapacityUnits"]
            }

    gsis = table.get("GlobalSecondaryIndexes")
    if gsis:
        gsi_list: List[Dict[str, Any]] = []
        for g in gsis:
            g_entry = {
                "IndexName": g.get("IndexName"),
                "KeySchema": g.get("KeySchema", []),
                "Projection": g.get("Projection", {"ProjectionType": "ALL"})
            }
            if billing != "PAY_PER_REQUEST" and "ProvisionedThroughput" in g:
                gpt = g["ProvisionedThroughput"]
                if "ReadCapacityUnits" in gpt and "WriteCapacityUnits" in gpt:
                    g_entry["ProvisionedThroughput"] = {
                        "ReadCapacityUnits": gpt["ReadCapacityUnits"],
                        "WriteCapacityUnits": gpt["WriteCapacityUnits"]
                    }
            gsi_list.append(g_entry)
        props["GlobalSecondaryIndexes"] = gsi_list

    lsis = table.get("LocalSecondaryIndexes")
    if lsis:
        lsi_list: List[Dict[str, Any]] = []
        for l in lsis:
            lsi_list.append({
                "IndexName": l.get("IndexName"),
                "KeySchema": l.get("KeySchema", []),
                "Projection": l.get("Projection", {"ProjectionType": "ALL"})
            })
        props["LocalSecondaryIndexes"] = lsi_list

    ss = table.get("StreamSpecification")
    if ss and ss.get("StreamEnabled"):
        props["StreamSpecification"] = {"StreamViewType": ss.get("StreamViewType", "NEW_IMAGE")}

    sse = table.get("SSEDescription") or table.get("SSESpecification")
    if isinstance(sse, dict) and (sse.get("Status") in ("ENABLED","ENABLING") or sse.get("SSEEnabled") is True):
        props["SSESpecification"] = {"SSEEnabled": True}

    cfn = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            logical_name: {
                "Type": "AWS::DynamoDB::Table",
                "Properties": props
            }
        }
    }
    return cfn

def process_single(path: Path, out: Path = None, logical_name_arg: str = None) -> Path:
    describe = read_json_with_encoding(path)
    table = extract_table(describe)
    logical = logical_name_arg or make_logical_name(table.get("TableName") or path.stem)
    cfn = convert_table_to_cfn(table, logical)
    if out is None:
        out = path.with_name((table.get("TableName") or path.stem) + "-cfn.json")
    out.write_text(json.dumps(cfn, indent=2), encoding="utf-8")
    return out

def main():
    p = argparse.ArgumentParser(description="Convert describe-table JSON (any common text encoding) into CloudFormation JSON")
    p.add_argument("input", help="Input file or directory")
    p.add_argument("-o","--output", help="Output file (for single input)")
    p.add_argument("--out-dir", help="Output directory (for directory input)")
    p.add_argument("--logical-name", help="Optional CloudFormation logical resource name")
    args = p.parse_args()

    inp = Path(args.input)
    if not inp.exists():
        raise SystemExit(f"Input not found: {inp}")

    if inp.is_dir():
        out_dir = Path(args.out_dir) if args.out_dir else inp
        out_dir.mkdir(parents=True, exist_ok=True)
        for f in inp.iterdir():
            if f.is_file() and f.suffix.lower() == ".json":
                try:
                    target = out_dir / (f.stem + "-cfn.json")
                    res = process_single(f, target, args.logical_name)
                    print("Wrote:", res)
                except Exception as e:
                    print(f"Skipped {f.name}: {e}")
    else:
        out = Path(args.output) if args.output else None
        res = process_single(inp, out, args.logical_name)
        print("Wrote:", res)

if __name__ == "__main__":
    main()