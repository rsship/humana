package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/xuri/excelize/v2"
)

type HumanaProcessor struct {
	inputFile     string
	outputFile    string
	headerMap     map[string]int    
	carrierRules  map[string]Rule   
	columnMapping map[string]string 
	buffer        *bufio.Writer     
}

type Rule struct {
	carrier string
	subline string
	check   func(map[string]string) bool
}

type ProcessedRow struct {
	data   []string
	rowNum int
}

func NewHumanaProcessor(input, output string) *HumanaProcessor {
	p := &HumanaProcessor{
		inputFile:     input,
		outputFile:    output,
		headerMap:     make(map[string]int),
		carrierRules:  make(map[string]Rule),
		columnMapping: make(map[string]string),
	}
	p.initializeRules()
	p.initializeColumnMapping()
	return p
}

func (p *HumanaProcessor) initializeRules() {
	p.carrierRules = map[string]Rule{
		"dental": {
			carrier: "Humana Dental",
			subline: "Dental",
			check: func(row map[string]string) bool {
				return strings.ToLower(strings.TrimSpace(row["Product"])) == "dental"
			},
		},
		"vision": {
			carrier: "Humana Vision",
			subline: "Vision",
			check: func(row map[string]string) bool {
				return strings.ToLower(strings.TrimSpace(row["Product"])) == "vision"
			},
		},
		"medsup": {
			carrier: "Humana Med Supp",
			subline: "Med Supp",
			check: func(row map[string]string) bool {
				return strings.ToLower(strings.TrimSpace(row["BlkBusCd"])) == "ms"
			},
		},
		"pdp": {
			carrier: "Humana PDP",
			subline: "PDP",
			check: func(row map[string]string) bool {
				blkBusCd := strings.ToLower(strings.TrimSpace(row["BlkBusCd"]))
				planType := strings.ToLower(strings.TrimSpace(row["PlanType"]))
				return blkBusCd == "pdp" || (blkBusCd == "ma" && planType == "pdp")
			},
		},
		"mapd": {
			carrier: "Humana MAPD",
			subline: "Med Adv",
			check: func(row map[string]string) bool {
				return strings.ToLower(strings.TrimSpace(row["BlkBusCd"])) == "ma"
			},
		},
	}
}

func (p *HumanaProcessor) initializeColumnMapping() {
	p.columnMapping = map[string]string{
		"AgentName":       "C",
		"AgentID":        "D",
		"StatementDate":  "B",
		"ClientFullName": "E",
		"CarrierMemberID": "F",
		"PolicyNumber":   "AM",
		"EffectiveDate":  "AF",
		"PlanType":       "T",
		"Contract":       "AN",
		"Premium":        "W",
		"AgentSplit":     "X",
		"CompRate":       "V",
		"Commission":     "Y",
		"CommAction":     "AB",
		"Product":        "S",
		"BlkBusCd":       "J",
	}
}

func (p *HumanaProcessor) ProcessFile() error {
	outFile, err := os.Create(p.outputFile)
	if err != nil {
		return fmt.Errorf("error creating output file: %w", err)
	}
	defer outFile.Close()

	p.buffer = bufio.NewWriterSize(outFile, 64*1024) 
	defer p.buffer.Flush()
	
	writer := csv.NewWriter(p.buffer)
	defer writer.Flush()
	
	if err := writer.Write(p.getHeaderRow()); err != nil {
		return fmt.Errorf("error writing header: %w", err)
	}
	
	f, err := excelize.OpenFile(p.inputFile)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()

	sheetName := f.GetSheetName(0)
	rows, err := f.Rows(sheetName)
	if err != nil {
		return fmt.Errorf("error getting rows: %w", err)
	}
	
	return p.processRowsConcurrently(rows, writer)
}

func (p *HumanaProcessor) processRowsConcurrently(rows *excelize.Rows, writer *csv.Writer) error {
	const workerCount = 4
	rowChan := make(chan []string, workerCount)
	resultChan := make(chan ProcessedRow, workerCount)
	errChan := make(chan error, 1)
	var wg sync.WaitGroup
	
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go p.processRowWorker(rowChan, resultChan, &wg)
	}
	
	var writeWg sync.WaitGroup
	writeWg.Add(1)
	go p.writeResults(writer, resultChan, &writeWg)
	
	if rows.Next() {
		_, err := rows.Columns()
		if err != nil {
			return fmt.Errorf("error reading header: %w", err)
		}
	}
	
	rowNum := 1
	for rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			errChan <- err
			break
		}
		if len(cols) > 0 && cols[2] != "" { 
			rowChan <- cols
			rowNum++
		}
	}
	
	close(rowChan)
	wg.Wait()
	close(resultChan)
	writeWg.Wait()

	
	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

func (p *HumanaProcessor) processRowWorker(rows <-chan []string, results chan<- ProcessedRow, wg *sync.WaitGroup) {
	defer wg.Done()

	for row := range rows {
		rowMap := p.rowToMap(row)
		processed := p.processRow(rowMap)
		results <- ProcessedRow{
			data:   processed,
			rowNum: 0, 
		}
	}
}

func (p *HumanaProcessor) writeResults(writer *csv.Writer, results <-chan ProcessedRow, wg *sync.WaitGroup) {
	defer wg.Done()

	for result := range results {
		if err := writer.Write(result.data); err != nil {
			log.Printf("Error writing row: %v", err)
			continue
		}
	}
}

func (p *HumanaProcessor) rowToMap(row []string) map[string]string {
	result := make(map[string]string)
	for key, col := range p.columnMapping {
		colIndex := p.getColumnIndex(col)
		if colIndex < len(row) {
			result[key] = row[colIndex]
		}
	}
	return result
}

func (p *HumanaProcessor) processRow(row map[string]string) []string {
	output := make([]string, 53) 
	output[16] = "Health" 
	p.mapBasicFields(output, row)
	
	carrier, subline := p.determineCarrierAndSubline(row)
	output[1] = carrier  
	output[17] = subline 
	output[32] = filepath.Base(p.inputFile)

	p.splitClientName(output, row["ClientFullName"])
	return output
}

func (p *HumanaProcessor) mapBasicFields(output []string, row map[string]string) {
	output[0] = p.formatAgentName(row["AgentName"])
	output[2] = row["AgentID"]
	output[4] = p.formatDate(row["StatementDate"])
	output[6] = row["ClientFullName"]
	output[10] = row["CarrierMemberID"]
	output[11] = row["PolicyNumber"]
	output[12] = p.formatDate(row["EffectiveDate"])
	output[18] = row["PlanType"]
	output[20] = row["Contract"]
	output[24] = row["Premium"]
	
	if split, err := strconv.ParseFloat(row["AgentSplit"], 64); err == nil {
		output[25] = fmt.Sprintf("%.4f", split/100)
	}

	output[26] = row["CompRate"]
	output[28] = row["Commission"]
	output[31] = row["CommAction"]
}

func (p *HumanaProcessor) determineCarrierAndSubline(row map[string]string) (string, string) {
	for _, rule := range p.carrierRules {
		if rule.check(row) {
			carrier := rule.carrier
			if strings.Contains(strings.ToLower(row["CommAction"]), "override") {
				carrier += " override"
			}
			return carrier, rule.subline
		}
	}
	
	carrier := "Humana"
	if strings.Contains(strings.ToLower(row["CommAction"]), "override") {
		carrier += " override"
	}
	return carrier, ""
}

func (p *HumanaProcessor) getColumnIndex(col string) int {
	if len(col) == 1 {
		return int(col[0] - 'A')
	}
	return (int(col[0]-'A'+1) * 26) + int(col[1]-'A')
}

func (p *HumanaProcessor) formatDate(date string) string {
	if date == "" {
		return ""
	}

	formats := [...]string{
		"2006-01-02",
		"1/2/2006",
		"01/02/2006",
		"2006/01/02",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, date); err == nil {
			return t.Format("01/02/2006")
		}
	}
	return date
}

func (p *HumanaProcessor) formatAgentName(name string) string {
	if name == "" {
		return ""
	}

	words := strings.Fields(name)
	for i, word := range words {
		if word == "" {
			continue
		}
		runes := []rune(word)
		words[i] = string(unicode.ToUpper(runes[0])) + strings.ToLower(string(runes[1:]))
	}
	return strings.Join(words, " ")
}

func (p *HumanaProcessor) splitClientName(output []string, fullName string) {
	if fullName == "" {
		return
	}

	names := strings.Fields(fullName)
	switch len(names) {
	case 0:
		return
	case 1:
		output[7] = p.formatAgentName(names[0]) 
	case 2:
		output[7] = p.formatAgentName(names[0]) 
		output[9] = p.formatAgentName(names[1]) 
	default:
		output[7] = p.formatAgentName(names[0])                      
		output[9] = p.formatAgentName(names[len(names)-1])          
		output[8] = p.formatAgentName(strings.Join(names[1:len(names)-1], " ")) 
	}
}

func (p *HumanaProcessor) getHeaderRow() []string {
	return []string{
		"Agent Name", "Carrier", "Agent ID", "Agent NPN", "Statement Date",
		"Payment Period", "Client Full Name", "Client First Name",
		"Client Middle Name/Initial", "Client Last name", "Carrier Member ID",
		"Policy Number", "Effective Date", "Prior Plan?", "Termination Date",
		"Termination Reason", "Line", "Sub-line", "Plan Type", "Plan",
		"Contract", "PBP", "Member State", "Member County", "Premium",
		"Agent Split", "Comp Rate", "Lives", "Commission", "Expected Comm",
		"Reconcile", "Commission Action", "Statement link", "Classification",
		"Agent Comp Plan", "Agent Payroll", "Apply Payroll To", "Upline 1 Name",
		"Upline 1 Comp Plan", "Upline 1 Payroll", "Upline 2 Name",
		"Upline 2 Comp Plan", "Upline 2 Payroll", "Upline 3 Name",
		"Upline 3 Comp Plan", "Upline 3 Payroll", "Upline 4 Name",
		"Upline 4 Comp Plan", "Upline 4 Payroll", "Upline 5 Name",
		"Upline 5 Comp Plan", "Upline 5 Payroll", "Your Spread",
	}
}

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Usage: program <input_xlsx_file> <output_csv_file>")
	}

	inputFile := os.Args[1]
	outputFile := os.Args[2]

	processor := NewHumanaProcessor(inputFile, outputFile)
	startTime := time.Now()
	
	if err := processor.ProcessFile(); err != nil {
		log.Fatalf("Error processing file: %v", err)
	}

	duration := time.Since(startTime)
	fmt.Printf("Successfully processed %s to %s in %v\n", inputFile, outputFile, duration)
}
