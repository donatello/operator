package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

const (
	auditLogEventsTable = "audit_log_events"
)

const (
	createAuditLogEventsTable = `CREATE TABLE %s (
                                       event_time TIMESTAMPTZ NOT NULL,
                                       log JSONB NOT NULL
                                     ) PARTITION BY RANGE (event_time);`
	createTablePartition = `CREATE TABLE %s PARTITION OF %s
                                  FOR VALUES FROM ('%s') TO ('%s');`
)

const (
	partitionsPerMonth = 4
)

func mkQuery(qTemplate string, args ...interface{}) string {
	return fmt.Sprintf(qTemplate, args...)
}

func getPartitionRange(t time.Time) (time.Time, time.Time) {
	// Zero out the time and use UTC
	t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	daysInMonth := t.AddDate(0, 1, -t.Day()).Day()
	quot := daysInMonth / partitionsPerMonth
	remDays := daysInMonth % partitionsPerMonth
	rangeStart := t.AddDate(0, 0, 1-t.Day())
	for {
		rangeDays := quot
		if remDays > 0 {
			rangeDays++
			remDays--
		}
		rangeEnd := rangeStart.AddDate(0, 0, rangeDays)
		if t.Before(rangeEnd) {
			return rangeStart, rangeEnd
		}
		rangeStart = rangeEnd
	}
}

type DBClient struct {
	*pgxpool.Pool
}

func NewDBClient(ctx context.Context, connStr string) (*DBClient, error) {
	pool, err := pgxpool.Connect(ctx, connStr)
	if err != nil {
		return nil, err
	}
	return &DBClient{pool}, nil
}

func (c *DBClient) checkTableExists(ctx context.Context, table string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	res, _ := c.Query(ctx, mkQuery(`SELECT 1 FROM %s WHERE false;`, table))
	if res.Err() != nil {
		if strings.Contains(res.Err().Error(), "(SQLSTATE 42P01)") {
			return false, nil
		}
		return false, res.Err()
	}
	return true, nil
}

// Create tables
func (c *DBClient) InitDBTables(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if exists, err := c.checkTableExists(ctx, auditLogEventsTable); err != nil {
		return err
	} else if exists {
		return nil
	}

	// Create all initial tables
	if _, err := c.Exec(ctx, mkQuery(createAuditLogEventsTable, auditLogEventsTable)); err != nil {
		return err
	}

	start, end := getPartitionRange(time.Now())
	tableName := fmt.Sprintf("%s_%s", auditLogEventsTable, start.Format("2006_01_02"))
	rangeStart, rangeEnd := start.Format("2006-01-02"), end.Format("2006-01-02")
	if _, err := c.Exec(ctx, mkQuery(createTablePartition, tableName, auditLogEventsTable, rangeStart, rangeEnd)); err != nil {
		return err
	}
	return nil
}

func (c *DBClient) InsertAuditLog(ctx context.Context, eventTime time.Time, logData string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	const insertAuditLogEvent = `INSERT INTO %s (event_time, log) VALUES ($1, $2);`

	_, err := c.Exec(ctx, mkQuery(insertAuditLogEvent, auditLogEventsTable), eventTime, logData)
	if err != nil {
		return err
	}

	return nil
}
