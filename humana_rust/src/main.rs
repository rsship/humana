use calamine::{open_workbook_auto, Data, Reader};
use chrono::NaiveDate;
use csv::Writer;
use rayon::prelude::*;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

type RowMap = HashMap<String, String>;

struct Rule {
    carrier: String,
    subline: String,
    check: Box<dyn Fn(&RowMap) -> bool + Sync + Send>,
}

struct HumanaProcessor {
    input_file: String,
    output_file: String,
    column_mapping: HashMap<String, String>,
    carrier_rules: Vec<Rule>,
}

impl HumanaProcessor {
    fn new(input: String, output: String) -> Self {
        let mut processor = Self {
            input_file: input,
            output_file: output,
            column_mapping: HashMap::new(),
            carrier_rules: Vec::new(),
        };
        processor.initialize_rules();
        processor.initialize_column_mapping();
        processor
    }

    fn initialize_rules(&mut self) {
        self.carrier_rules = vec![
            Rule {
                carrier: "Humana Dental".into(),
                subline: "Dental".into(),
                check: Box::new(|row: &RowMap| {
                    row.get("Product")
                        .map(|s| s.trim().to_lowercase() == "dental")
                        .unwrap_or(false)
                }),
            },
            Rule {
                carrier: "Humana Vision".into(),
                subline: "Vision".into(),
                check: Box::new(|row: &RowMap| {
                    row.get("Product")
                        .map(|s| s.trim().to_lowercase() == "vision")
                        .unwrap_or(false)
                }),
            },
            Rule {
                carrier: "Humana Med Supp".into(),
                subline: "Med Supp".into(),
                check: Box::new(|row: &RowMap| {
                    row.get("BlkBusCd")
                        .map(|s| s.trim().to_lowercase() == "ms")
                        .unwrap_or(false)
                }),
            },
            Rule {
                carrier: "Humana PDP".into(),
                subline: "PDP".into(),
                check: Box::new(|row: &RowMap| {
                    let blk = row.get("BlkBusCd").map(|s| s.trim().to_lowercase());
                    let plan = row.get("PlanType").map(|s| s.trim().to_lowercase());
                    match (blk, plan) {
                        (Some(ref b), _) if b == "pdp" => true,
                        (Some(ref b), Some(ref p)) if b == "ma" && p == "pdp" => true,
                        _ => false,
                    }
                }),
            },
            Rule {
                carrier: "Humana MAPD".into(),
                subline: "Med Adv".into(),
                check: Box::new(|row: &RowMap| {
                    row.get("BlkBusCd")
                        .map(|s| s.trim().to_lowercase() == "ma")
                        .unwrap_or(false)
                }),
            },
        ];
    }

    fn initialize_column_mapping(&mut self) {
        let mapping = [
            ("AgentName", "C"),
            ("AgentID", "D"),
            ("StatementDate", "B"),
            ("ClientFullName", "E"),
            ("CarrierMemberID", "F"),
            ("PolicyNumber", "AM"),
            ("EffectiveDate", "AF"),
            ("PlanType", "T"),
            ("Contract", "AN"),
            ("Premium", "W"),
            ("AgentSplit", "X"),
            ("CompRate", "V"),
            ("Commission", "Y"),
            ("CommAction", "AB"),
            ("Product", "S"),
            ("BlkBusCd", "J"),
        ];
        for (k, v) in mapping.iter() {
            self.column_mapping.insert((*k).to_string(), v.to_string());
        }
    }

    fn get_header_row() -> Vec<&'static str> {
        vec![
            "Agent Name",
            "Carrier",
            "Agent ID",
            "Agent NPN",
            "Statement Date",
            "Payment Period",
            "Client Full Name",
            "Client First Name",
            "Client Middle Name/Initial",
            "Client Last name",
            "Carrier Member ID",
            "Policy Number",
            "Effective Date",
            "Prior Plan?",
            "Termination Date",
            "Termination Reason",
            "Line",
            "Sub-line",
            "Plan Type",
            "Plan",
            "Contract",
            "PBP",
            "Member State",
            "Member County",
            "Premium",
            "Agent Split",
            "Comp Rate",
            "Lives",
            "Commission",
            "Expected Comm",
            "Reconcile",
            "Commission Action",
            "Statement link",
            "Classification",
            "Agent Comp Plan",
            "Agent Payroll",
            "Apply Payroll To",
            "Upline 1 Name",
            "Upline 1 Comp Plan",
            "Upline 1 Payroll",
            "Upline 2 Name",
            "Upline 2 Comp Plan",
            "Upline 2 Payroll",
            "Upline 3 Name",
            "Upline 3 Comp Plan",
            "Upline 3 Payroll",
            "Upline 4 Name",
            "Upline 4 Comp Plan",
            "Upline 4 Payroll",
            "Upline 5 Name",
            "Upline 5 Comp Plan",
            "Upline 5 Payroll",
            "Your Spread",
        ]
    }

    fn process_file(&self) -> Result<(), Box<dyn std::error::Error>> {
        let output_file = File::create(&self.output_file)?;
        let mut writer = Writer::from_writer(BufWriter::with_capacity(64 * 1024, output_file));

        writer.write_record(Self::get_header_row())?;

        let mut workbook = open_workbook_auto(&self.input_file)?;

        let sheet_name = workbook
            .sheet_names()
            .get(0)
            .ok_or("No sheet found")?
            .to_string();
        let range = workbook.worksheet_range(&sheet_name)?;

        let data_rows: Vec<Vec<String>> = range
            .rows()
            .skip(1)
            .filter(|row| row.len() > 2 && row[2] != "")
            .map(|row| {
                row.iter()
                    .map(|cell| match cell {
                        Data::String(s) => s.clone(),
                        Data::Float(f) => f.to_string(),
                        Data::Int(i) => i.to_string(),
                        Data::Bool(b) => b.to_string(),
                        _ => "".to_string(),
                    })
                    .collect::<Vec<String>>()
            })
            .collect();

        let arc_self = Arc::new(self);

        let processed_rows: Vec<Vec<String>> = data_rows
            .par_iter()
            .map(|row| arc_self.process_row(row))
            .collect();

        for row in processed_rows {
            writer.write_record(&row)?;
        }
        writer.flush()?;
        Ok(())
    }

    fn process_row(&self, row: &[String]) -> Vec<String> {
        let mut output = vec![String::new(); 53];

        output[16] = "Health".to_string();

        let row_map = self.row_to_map(row);
        self.map_basic_fields(&mut output, &row_map);

        let (carrier, subline) = self.determine_carrier_and_subline(&row_map);
        output[1] = carrier;
        output[17] = subline;

        if let Some(fname) = Path::new(&self.input_file).file_name() {
            output[32] = fname.to_string_lossy().into();
        }

        self.split_client_name(
            &mut output,
            row_map.get("ClientFullName").unwrap_or(&"".to_string()),
        );
        output
    }

    fn row_to_map(&self, row: &[String]) -> RowMap {
        let mut result = HashMap::new();
        for (key, col) in &self.column_mapping {
            let index = Self::get_column_index(col);
            if index < row.len() {
                result.insert(key.clone(), row[index].clone());
            }
        }
        result
    }

    fn map_basic_fields(&self, output: &mut Vec<String>, row: &RowMap) {
        output[0] = Self::format_agent_name(row.get("AgentName").unwrap_or(&"".into()));
        output[2] = row.get("AgentID").cloned().unwrap_or_default();
        output[4] = Self::format_date(row.get("StatementDate").unwrap_or(&"".into()));
        output[6] = row.get("ClientFullName").cloned().unwrap_or_default();
        output[10] = row.get("CarrierMemberID").cloned().unwrap_or_default();
        output[11] = row.get("PolicyNumber").cloned().unwrap_or_default();
        output[12] = Self::format_date(row.get("EffectiveDate").unwrap_or(&"".into()));
        output[18] = row.get("PlanType").cloned().unwrap_or_default();
        output[20] = row.get("Contract").cloned().unwrap_or_default();
        output[24] = row.get("Premium").cloned().unwrap_or_default();

        if let Ok(split) = row.get("AgentSplit").unwrap_or(&"".into()).parse::<f64>() {
            output[25] = format!("{:.4}", split / 100.0);
        }
        output[26] = row.get("CompRate").cloned().unwrap_or_default();
        output[28] = row.get("Commission").cloned().unwrap_or_default();
        output[31] = row.get("CommAction").cloned().unwrap_or_default();
    }

    fn determine_carrier_and_subline(&self, row: &RowMap) -> (String, String) {
        for rule in &self.carrier_rules {
            if (rule.check)(row) {
                let mut carrier = rule.carrier.clone();
                if row
                    .get("CommAction")
                    .map(|s| s.to_lowercase().contains("override"))
                    .unwrap_or(false)
                {
                    carrier.push_str(" override");
                }
                return (carrier, rule.subline.clone());
            }
        }
        let mut carrier = "Humana".to_string();
        if row
            .get("CommAction")
            .map(|s| s.to_lowercase().contains("override"))
            .unwrap_or(false)
        {
            carrier.push_str(" override");
        }
        (carrier, "".to_string())
    }

    fn get_column_index(col: &str) -> usize {
        let chars: Vec<char> = col.chars().collect();
        if chars.len() == 1 {
            (chars[0] as u8 - b'A') as usize
        } else if chars.len() == 2 {
            (((chars[0] as u8 - b'A' + 1) as usize) * 26) + ((chars[1] as u8 - b'A') as usize)
        } else {
            0
        }
    }

    fn format_date(date: &str) -> String {
        if date.is_empty() {
            return "".to_string();
        }
        let formats = ["%Y-%m-%d", "%-m/%-d/%Y", "%m/%d/%Y", "%Y/%m/%d"];
        for fmt in &formats {
            if let Ok(parsed) = NaiveDate::parse_from_str(date, fmt) {
                return parsed.format("%m/%d/%Y").to_string();
            }
        }
        date.to_string()
    }

    fn format_agent_name(name: &str) -> String {
        if name.is_empty() {
            return "".to_string();
        }
        name.split_whitespace()
            .map(|word| {
                let mut c = word.chars();
                if let Some(first) = c.next() {
                    first.to_uppercase().to_string() + &c.as_str().to_lowercase()
                } else {
                    "".to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn split_client_name(&self, output: &mut Vec<String>, full_name: &str) {
        if full_name.is_empty() {
            return;
        }
        let names: Vec<&str> = full_name.split_whitespace().collect();
        match names.len() {
            0 => {}
            1 => {
                output[7] = Self::format_agent_name(names[0]);
            }
            2 => {
                output[7] = Self::format_agent_name(names[0]);
                output[9] = Self::format_agent_name(names[1]);
            }
            _ => {
                output[7] = Self::format_agent_name(names[0]);
                output[9] = Self::format_agent_name(names[names.len() - 1]);
                output[8] = Self::format_agent_name(&names[1..names.len() - 1].join(" "));
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: program <input_xlsx_file> <output_csv_file>");
        std::process::exit(1);
    }
    let input_file = args[1].clone();
    let output_file = args[2].clone();

    let processor = HumanaProcessor::new(input_file.clone(), output_file.clone());
    let start = std::time::Instant::now();
    processor.process_file()?;
    let duration = start.elapsed();
    println!(
        "Successfully processed {} to {} in {:?}",
        input_file, output_file, duration
    );
    Ok(())
}
