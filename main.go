package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"strings"
	"sync"
	"time"
)

var (
	sourceUrl = "postgres://postgres:123456@192.168.116.128:5433/postgres"
	targetUrl = "postgres://postgres:123456@192.168.116.128:5434/postgres"
	sourceDb  *pgx.Conn
	targetDb  *pgxpool.Pool
)

func main() {
	var err error
	sourceDb, err = pgx.Connect(context.Background(), sourceUrl)
	if err != nil {
		log.Fatalf("Unable to connect to source database: %v\n", err)
	}
	defer sourceDb.Close(context.Background())
	targetDb, err = pgxpool.New(context.Background(), targetUrl)
	if err != nil {
		log.Fatalf("Unable to create target database connection pool: %v\n", err)
	}
	defer targetDb.Close()

	i := 0
	for {
		pid := getProgress()
		data, pid := getChanges(pid)
		if len(data) == 0 {
			// There is no data that needs to be synced
			time.Sleep(5 * time.Second)
			i++
			if i >= 12 {
				i = 0
				log.Println("sync finished")
			}
			continue
		}
		i = 0
		wg := new(sync.WaitGroup)
		for _, row := range data {
			wg.Add(1)
			go func() {
				sql := generateSql(row)
				_, err := targetDb.Exec(context.Background(), sql)
				if err != nil {
					log.Println(err)
				}
				wg.Done()
			}()
		}
		wg.Wait()
		setProgress(pid)
	}
}

// Obtain the ID of the last sync
func getProgress() (progress int) {
	err := targetDb.QueryRow(context.Background(), "select progress from progress where id = 1").Scan(&progress)
	if err != nil {
		log.Fatalf("Unable to get progress: %v\n", err)
	}
	return
}

// Update the id of the last sync
func setProgress(progress int) {
	_, err := targetDb.Exec(context.Background(), "update progress set progress = $1 where id = 1", progress)
	if err != nil {
		log.Printf("Unable to update progress: %v\n", err)
	}
}

type syncLog struct {
	Id        int                    `db:"id"`
	TableName string                 `db:"table_name"`
	Operation string                 `db:"operation"`
	Data      map[string]interface{} `db:"data"`
}

func getChanges(pid int) (map[string]syncLog, int) {
	rows, _ := sourceDb.Query(context.Background(), "select id, table_name, operation, data from sync_log where id > $1 order by id limit 100", pid)
	defer rows.Close()
	data := make(map[string]syncLog)
	for rows.Next() {
		var item syncLog
		_ = rows.Scan(&item.Id, &item.TableName, &item.Operation, &item.Data)
		pid = item.Id
		id := item.Data["id"]
		key := fmt.Sprintf("%v_%v", item.TableName, id)
		item.Id = int(id.(float64))
		data[key] = item
	}
	return data, pid
}

func generateSql(row syncLog) string {
	if row.Operation == "DELETE" {
		return fmt.Sprintf("delete from %v where id = '%v'", row.TableName, row.Id)
	}
	l := len(row.Data)
	keys := make([]string, 0, l)
	values := make([]string, 0, l)
	update := make([]string, 0, l-1)
	for k, v := range row.Data {
		if _, ok := v.([]interface{}); ok {
			bytes, _ := json.Marshal(v)
			v = string(bytes)
		}
		if _, ok := v.(map[string]interface{}); ok {
			bytes, _ := json.Marshal(v)
			v = string(bytes)
		}
		if _, ok := v.(string); ok {
			v = strings.Replace(v.(string), "'", "''", -1)
		}

		keys = append(keys, k)
		values = append(values, fmt.Sprintf("'%v'", v))
		if k != "id" {
			update = append(update, fmt.Sprintf("%v = excluded.%v", k, k))
		}
	}
	return fmt.Sprintf("insert into %v (%v) values (%v) on conflict (id) do update set %v", row.TableName, strings.Join(keys, ","), strings.Join(values, ","), strings.Join(update, ","))
}
