// This program serves to reproduce this problem:
// https://stackoverflow.com/questions/65253798/go-sql-driver-get-invalid-connection-when-wait-timeout-is-8h-as-default
//
// As was stated in the question:
//
// The **intention** is to "read all records from table A, make some transformation, and write the resulted records to table B".
// **Implemented** by
// + One goroutine read lines from a web page (as a mock of scanning table A),
// generating records and putting them into a channel.
// + Other four goroutine (number configurable) concurrently consume from above channel,
// accumulating 50 rows (batch size configurable) to insert into table B,
// then accumulating another 50 rows, and so on so forth.
//
// Table B DDL:
//
//      CREATE TABLE `table_b` (
//       `id` bigint(20) NOT NULL AUTO_INCREMENT,
//       `value` text NOT NULL,
//       PRIMARY KEY (`id`)
//     ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
//
// Output/ log:
// A file named like 'log_2020_12_13_00_36_40' will be created when running this program.
// Attached is a sample log file of my run.
// The error occurred once in this run:
// ```
// 2020/12/13 00:38:03 main.go:149: [goroutine 8] batchInsert: db.Exec(): invalid connection
// sql.DB Stats={MaxOpenConnections:50 OpenConnections:4 InUse:0 Idle:4 WaitCount:0 WaitDuration:0s MaxIdleClosed:0 MaxLifetimeClosed:0}
// ```
//
// Note with `dsn`:
// Although I wrote `localhost:3306` in the code, the error was only reproduced when I connected to
// a **remote** MySQL server.
//
package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	sq "github.com/lann/squirrel"
)

var (
	totalRecords   = flag.Int("total", 262144, "specify expected total records of table_a")
	chanSize       = flag.Int("chSz", 100000, "channel size")
	writeWorkerNum = flag.Int("writeNum", 4, "goroutine numbers to write table_b")
	writeBatchSize = flag.Int("writeBatch", 50, "write batch size")
)

var (
	recordsCh chan *TableADto
	db        *sql.DB
	gortIds   []int
	wg        sync.WaitGroup
	logger    *log.Logger
)

type TableADto struct {
	ID    int64
	Value string
}

type TableBDto TableADto

func main() {
	flag.Parse()
	Init()

	go scanTableA()

	concurWriteTableB()

	time.Sleep(5 * time.Second)
	wg.Wait()
}

var readanovel = func() []string {
	lines := make([]string, 0)

	resp, err := http.Get(`http://gutenberg.net.au/ebooks02/0200161.txt`)
	//resp, err := http.Get(`https://archive.org/stream/davidcopperfield00766gut/766.txt`)
	checkErr(err, "http.Get()")
	defer resp.Body.Close()

	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}

	return lines
}

// scanning `table_a` is mocked by reading some content from a web page
func scanTableA() {
	// avoid program quitting unexpectedly in case if writer is faster than scanner
	wg.Add(128)
	defer wg.Add(-128)

	lines := readanovel()
	n := len(lines)
	i := 0
	for ; i < *totalRecords;  {
		aDto := TableADto{Value: lines[i % n]}
		recordsCh <- &aDto
		wg.Add(1)
		i++

		if i%1000 == 0 {
			logger.Printf("mock scanning table_a: got [%d, %v) records\n", i-1000, i)
		}
	}

	if r := i % 1000; r > 0 {
		logger.Printf("mock scanning table_a: got [%d, %v) records. Scanning done!\n", i-r, i)
	}
	close(recordsCh)
}

func concurWriteTableB() {
	var execute = func() {
		currGoId := goid()
		gortIds = append(gortIds, currGoId)

		bDtos := make([]*TableBDto, 0, *writeBatchSize)
		for record := range recordsCh {
			if len(bDtos) == *writeBatchSize {
				batchInsertWithLog(currGoId, bDtos)
				bDtos = []*TableBDto{}

				// let one goroutine sleep some seconds, hoping to make the issue easier to reproduce
				//if currGoId == gortIds[0] {
				//	n := rand.Intn(60)
				//	time.Sleep(time.Duration(n) * time.Second)
				//}
			}

			bDtos = append(bDtos, transform(record))
		}

		// handle those not enough for a batch
		if len(bDtos) > 0 {
			batchInsertWithLog(currGoId, bDtos)
		}
	}

	for i := 0; i < *writeWorkerNum; i++ {
		go execute()
	}
}

func transform(aDto *TableADto) *TableBDto {
	bDto := new(TableBDto)
	// pretending there is some transformation here ...
	bDto.Value = aDto.Value
	return bDto
}

func batchInsertWithLog(goId int, bDtos []*TableBDto) {
	// simulate the elapse when talking to a remote mysql server
	// skipped because finally I used a real remote server
	//defer func() {
	//	mockElapse := time.Duration(500+rand.Intn(100)) * time.Millisecond
	//	time.Sleep(mockElapse)
	//}()

	if _, err := batchInsert(bDtos); err != nil {
		logger.Printf("[goroutine %v] batchInsert: %v \nsql.DB Stats=%+v\n", goId, err, db.Stats())
	} else {
		logger.Printf("[goroutine %v] batchInsert: Success! \nsql.DB Stats=%+v\n", goId, db.Stats())
	}
	wg.Add(-1 * len(bDtos))
}

func batchInsert(bDtos []*TableBDto) (int64, error) {
	sb := sq.Insert("table_b").Columns("value")
	for _, b := range bDtos {
		sb = sb.Values(b.Value)
	}

	sql, args, err := sb.ToSql()
	checkErr(err, "sb.ToSql()")
	ret, err := db.Exec(sql, args...)
	if err != nil {
		return -1, fmt.Errorf("db.Exec(): %v", err)
	}

	return ret.RowsAffected()
}

func Init() {
	var err error

	logfile, err := os.Create(fmt.Sprintf("log_%v", time.Now().Format("2006_01_02_15_04_05")))
	checkErr(err, "os.Create() log file")
	logger = log.New(logfile, "", log.LstdFlags|log.Lshortfile)

	recordsCh = make(chan *TableADto, *chanSize)

	db, err = sql.Open("mysql", `poi_it:QGW3RKHVGXPAJ1OI@tcp(stg-poi-it-aurora-cluster.cluster-czmsfje8s8ya.ap-southeast-1.rds.amazonaws.com:3306)/poi_it?parseTime=true`)
	//db, err = sql.Open("mysql", `root:@tcp(localhost:3306)/practice?parseTime=true`)
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(10)
	checkErr(err, "sql.Open()")
	checkErr(db.Ping(), "db.Ping()")
}

// get current goroutine id
func goid() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

func checkErr(err error, prompt string) {
	if err != nil {
		log.Fatalf("%s: %v\n", prompt, err)
	}
}
