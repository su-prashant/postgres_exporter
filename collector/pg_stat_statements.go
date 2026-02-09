// Copyright 2023 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/alecthomas/kingpin/v2"
	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	statStatementsSubsystem = "stat_statements"
	defaultStatementLimit   = "100"
)

var (
	includeQueryFlag      *bool   = nil
	statementLengthFlag   *uint   = nil
	statementLimitFlag    *uint   = nil
	excludedDatabasesFlag *string = nil
	excludedUsersFlag     *string = nil
	includeAllMetricsFlag *bool   = nil
)

func init() {
	// WARNING:
	//   Disabled by default because this set of metrics can be quite expensive on a busy server
	//   Every unique query will cause a new timeseries to be created
	registerCollector(statStatementsSubsystem, defaultDisabled, NewPGStatStatementsCollector)

	includeAllMetricsFlag = kingpin.Flag(
		fmt.Sprint(collectorFlagPrefix, statStatementsSubsystem, ".include_all_metrics"),
		"Enable all metrics of the stat statements table(default: disabled)").
		Default(fmt.Sprintf("%v", defaultDisabled)).
		Bool()
	includeQueryFlag = kingpin.Flag(
		fmt.Sprint(collectorFlagPrefix, statStatementsSubsystem, ".include_query"),
		"Enable selecting statement query together with queryId. (default: disabled)").
		Default(fmt.Sprintf("%v", defaultDisabled)).
		Bool()
	statementLengthFlag = kingpin.Flag(
		fmt.Sprint(collectorFlagPrefix, statStatementsSubsystem, ".query_length"),
		"Maximum length of the statement text.").
		Default("120").
		Uint()
	statementLimitFlag = kingpin.Flag(
		fmt.Sprint(collectorFlagPrefix, statStatementsSubsystem, ".limit"),
		"Maximum number of statements to return.").
		Default(defaultStatementLimit).
		Uint()
	excludedDatabasesFlag = kingpin.Flag(
		fmt.Sprint(collectorFlagPrefix, statStatementsSubsystem, ".exclude_databases"),
		"Comma-separated list of database names to exclude. (default: none)").
		Default("").
		String()
	excludedUsersFlag = kingpin.Flag(
		fmt.Sprint(collectorFlagPrefix, statStatementsSubsystem, ".exclude_users"),
		"Comma-separated list of user names to exclude. (default: none)").
		Default("").
		String()
}

type PGStatStatementsCollector struct {
	log                   *slog.Logger
	includeQueryStatement bool
	includeAllMetrics     bool
	statementLength       uint
	statementLimit        uint
	excludedDatabases     []string
	excludedUsers         []string
}

func NewPGStatStatementsCollector(config collectorConfig) (Collector, error) {
	var excludedDatabases []string
	if *excludedDatabasesFlag != "" {
		for db := range strings.SplitSeq(*excludedDatabasesFlag, ",") {
			if trimmed := strings.TrimSpace(db); trimmed != "" {
				excludedDatabases = append(excludedDatabases, trimmed)
			}
		}
	}

	var excludedUsers []string
	if *excludedUsersFlag != "" {
		for user := range strings.SplitSeq(*excludedUsersFlag, ",") {
			if trimmed := strings.TrimSpace(user); trimmed != "" {
				excludedUsers = append(excludedUsers, trimmed)
			}
		}
	}

	return &PGStatStatementsCollector{
		log:                   config.logger,
		includeQueryStatement: *includeQueryFlag,
		includeAllMetrics:     *includeAllMetricsFlag,
		statementLength:       *statementLengthFlag,
		statementLimit:        *statementLimitFlag,
		excludedDatabases:     excludedDatabases,
		excludedUsers:         excludedUsers,
	}, nil
}

var (
	statStatementsCallsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "calls_total"),
		"Number of times executed",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	statStatementsSecondsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "seconds_total"),
		"Total time spent in the statement, in seconds",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	statStatementsRowsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "rows_total"),
		"Total number of rows retrieved or affected by the statement",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	statStatementsBlockReadSecondsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "block_read_seconds_total"),
		"Total time the statement spent reading blocks, in seconds",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	statStatementsBlockWriteSecondsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "block_write_seconds_total"),
		"Total time the statement spent writing blocks, in seconds",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)

	statStatementsSharedBlocksHit = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "shared_blocks_hit"),
		"Total number of shared block cache hits by the statement",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)

	statStatementsSharedBlocksRead = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "shared_blocks_read"),
		"Total number of shared blocks read by the statement",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)

	statStatementsSharedBlocksWritten = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "shared_blocks_written"),
		"Total number of shared blocks written by the statement",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)

	statStatementsSharedBlocksDirtied = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "shared_blocks_dirtied"),
		"Total number of shared blocks dirtied by the statement",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)

	statStatementsLocalBlocksHit = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "local_blocks_hit"),
		"Total number of local block cache hits by the statement",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)

	statStatementsLocalBlocksRead = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "local_blocks_read"),
		"Total number of local blocks read by the statement",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)

	statStatementsLocalBlocksDirtied = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "local_blocks_dirtied"),
		"Total number of local blocks dirtied by the statement",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)

	statStatementsLocalBlocksWritten = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "local_blocks_written"),
		"Total number of local blocks written by the statement",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)

	statStatementsTempBlocksRead = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "temp_blocks_read"),
		"Total number of temp blocks read by the statement",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)

	statStatementsTempBlocksWritten = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "temp_blocks_written"),
		"Total number of temp blocks written by the statement",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)

	statStatementsWalRecords = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "wal_records"),
		"Number of WAL records generated by the statement",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)

	statStatementsWalFpi = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "wal_fpi"),
		"Number of WAL full page images generated by the statement",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)

	statStatementsQuery = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "query_id"),
		"SQL Query to queryid mapping",
		[]string{"queryid", "query"},
		prometheus.Labels{},
	)
)

const (
	pgStatStatementQuerySelect      = `LEFT(pg_stat_statements.query, %d) as query,`
	pgStatStatementExcludeDatabases = `AND pg_database.datname NOT IN (%s) `
	pgStatStatementExcludeUsers     = `AND pg_get_userbyid(userid) NOT IN (%s) `

	pgStatStatementsQuery = `SELECT
		pg_get_userbyid(userid) as user,
		pg_database.datname,
		pg_stat_statements.queryid,
		%s
		pg_stat_statements.calls as calls_total,
		pg_stat_statements.total_time / 1000.0 as seconds_total,
		pg_stat_statements.rows as rows_total,
		pg_stat_statements.blk_read_time / 1000.0 as block_read_seconds_total,
		pg_stat_statements.blk_write_time / 1000.0 as block_write_seconds_total
		FROM pg_stat_statements
	JOIN pg_database
		ON pg_database.oid = pg_stat_statements.dbid
	WHERE
		total_time > (
		SELECT percentile_cont(0.1)
			WITHIN GROUP (ORDER BY total_time)
			FROM pg_stat_statements
		)
		%s %s
	ORDER BY seconds_total DESC
	LIMIT %s;`

	pgStatStatementsQuery_PG13 = `SELECT
		pg_get_userbyid(userid) as user,
		pg_database.datname,
		pg_stat_statements.queryid,
		%s
		pg_stat_statements.calls as calls_total,
		pg_stat_statements.total_exec_time / 1000.0 as seconds_total,
		pg_stat_statements.rows as rows_total,
		pg_stat_statements.blk_read_time / 1000.0 as block_read_seconds_total,
		pg_stat_statements.blk_write_time / 1000.0 as block_write_seconds_total,
		pg_stat_statements.shared_blks_hit,
		pg_stat_statements.shared_blks_read,
		pg_stat_statements.shared_blks_dirtied,
		pg_stat_statements.shared_blks_written,
		pg_stat_statements.local_blks_hit,
		pg_stat_statements.local_blks_read,
		pg_stat_statements.local_blks_dirtied,
		pg_stat_statements.local_blks_written,
		pg_stat_statements.temp_blks_read,
		pg_stat_statements.temp_blks_written,
		pg_stat_statements.wal_records,
		pg_stat_statements.wal_fpi
		FROM pg_stat_statements
	JOIN pg_database
		ON pg_database.oid = pg_stat_statements.dbid
	WHERE
		total_exec_time > (
		SELECT percentile_cont(0.1)
			WITHIN GROUP (ORDER BY total_exec_time)
			FROM pg_stat_statements
		)
		%s %s
	ORDER BY seconds_total DESC
	LIMIT %s;`

	pgStatStatementsQuery_PG17 = `SELECT
		pg_get_userbyid(userid) as user,
		pg_database.datname,
		pg_stat_statements.queryid,
		%s
		pg_stat_statements.calls as calls_total,
		pg_stat_statements.total_exec_time / 1000.0 as seconds_total,
		pg_stat_statements.rows as rows_total,
		pg_stat_statements.shared_blk_read_time / 1000.0 as block_read_seconds_total,
		pg_stat_statements.shared_blk_write_time / 1000.0 as block_write_seconds_total,
		pg_stat_statements.shared_blks_hit,
		pg_stat_statements.shared_blks_read,
		pg_stat_statements.shared_blks_dirtied,
		pg_stat_statements.shared_blks_written,
		pg_stat_statements.local_blks_hit,
		pg_stat_statements.local_blks_read,
		pg_stat_statements.local_blks_dirtied,
		pg_stat_statements.local_blks_written,
		pg_stat_statements.temp_blks_read,
		pg_stat_statements.temp_blks_written,
		pg_stat_statements.wal_records,
		pg_stat_statements.wal_fpi
		FROM pg_stat_statements
	JOIN pg_database
		ON pg_database.oid = pg_stat_statements.dbid
	WHERE
		total_exec_time > (
		SELECT percentile_cont(0.1)
			WITHIN GROUP (ORDER BY total_exec_time)
			FROM pg_stat_statements
		)
		%s %s
	ORDER BY seconds_total DESC
	LIMIT %s;`
)

func (c PGStatStatementsCollector) Update(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	var queryTemplate string
	switch {
	case instance.version.GE(semver.MustParse("17.0.0")):
		queryTemplate = pgStatStatementsQuery_PG17
	case instance.version.GE(semver.MustParse("13.0.0")):
		queryTemplate = pgStatStatementsQuery_PG13
	default:
		queryTemplate = pgStatStatementsQuery
	}
	querySelect := ""
	if c.includeQueryStatement {
		querySelect = fmt.Sprintf(pgStatStatementQuerySelect, c.statementLength)
	}
	databaseFilter := c.buildExclusionClause(c.excludedDatabases, pgStatStatementExcludeDatabases)
	userFilter := c.buildExclusionClause(c.excludedUsers, pgStatStatementExcludeUsers)
	statementLimit := defaultStatementLimit
	if c.statementLimit > 0 {
		statementLimit = fmt.Sprintf("%d", c.statementLimit)
	}
	query := fmt.Sprintf(queryTemplate, querySelect, databaseFilter, userFilter, statementLimit)

	db := instance.getDB()
	rows, err := db.QueryContext(ctx, query)

	presentQueryIds := make(map[string]struct{})

	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var user, datname, queryid, statement sql.NullString
		var callsTotal, rowsTotal sql.NullInt64
		var secondsTotal, blockReadSecondsTotal, blockWriteSecondsTotal sql.NullFloat64
		var shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written, wal_records, wal_fpi sql.NullInt64
		var columns []any
		if c.includeQueryStatement {
			columns = []any{&user, &datname, &queryid, &statement, &callsTotal, &secondsTotal, &rowsTotal, &blockReadSecondsTotal, &blockWriteSecondsTotal, &shared_blks_hit, &shared_blks_read, &shared_blks_dirtied, &shared_blks_written, &local_blks_hit, &local_blks_read, &local_blks_dirtied, &local_blks_written, &temp_blks_read, &temp_blks_written, &wal_records, &wal_fpi}
		} else {
			columns = []any{&user, &datname, &queryid, &callsTotal, &secondsTotal, &rowsTotal, &blockReadSecondsTotal, &blockWriteSecondsTotal, &shared_blks_hit, &shared_blks_read, &shared_blks_dirtied, &shared_blks_written, &local_blks_hit, &local_blks_read, &local_blks_dirtied, &local_blks_written, &temp_blks_read, &temp_blks_written, &wal_records, &wal_fpi}
		}
		if err := rows.Scan(columns...); err != nil {
			return err
		}

		userLabel := "unknown"
		if user.Valid {
			userLabel = user.String
		}
		datnameLabel := "unknown"
		if datname.Valid {
			datnameLabel = datname.String
		}
		queryidLabel := "unknown"
		if queryid.Valid {
			queryidLabel = queryid.String
		}

		callsTotalMetric := 0.0
		if callsTotal.Valid {
			callsTotalMetric = float64(callsTotal.Int64)
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsCallsTotal,
			prometheus.CounterValue,
			callsTotalMetric,
			userLabel, datnameLabel, queryidLabel,
		)

		secondsTotalMetric := 0.0
		if secondsTotal.Valid {
			secondsTotalMetric = secondsTotal.Float64
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsSecondsTotal,
			prometheus.CounterValue,
			secondsTotalMetric,
			userLabel, datnameLabel, queryidLabel,
		)

		rowsTotalMetric := 0.0
		if rowsTotal.Valid {
			rowsTotalMetric = float64(rowsTotal.Int64)
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsRowsTotal,
			prometheus.CounterValue,
			rowsTotalMetric,
			userLabel, datnameLabel, queryidLabel,
		)

		blockReadSecondsTotalMetric := 0.0
		if blockReadSecondsTotal.Valid {
			blockReadSecondsTotalMetric = blockReadSecondsTotal.Float64
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsBlockReadSecondsTotal,
			prometheus.CounterValue,
			blockReadSecondsTotalMetric,
			userLabel, datnameLabel, queryidLabel,
		)

		blockWriteSecondsTotalMetric := 0.0
		if blockWriteSecondsTotal.Valid {
			blockWriteSecondsTotalMetric = blockWriteSecondsTotal.Float64
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsBlockWriteSecondsTotal,
			prometheus.CounterValue,
			blockWriteSecondsTotalMetric,
			userLabel, datnameLabel, queryidLabel,
		)

		if c.includeAllMetrics {
			sharedBlocksHit := 0.0
			if shared_blks_hit.Valid {
				sharedBlocksHit = float64(shared_blks_hit.Int64)
			}
			ch <- prometheus.MustNewConstMetric(
				statStatementsSharedBlocksHit,
				prometheus.CounterValue,
				sharedBlocksHit,
				userLabel, datnameLabel, queryidLabel,
			)

			sharedBlocksRead := 0.0
			if shared_blks_read.Valid {
				sharedBlocksRead = float64(shared_blks_read.Int64)
			}
			ch <- prometheus.MustNewConstMetric(
				statStatementsSharedBlocksRead,
				prometheus.CounterValue,
				sharedBlocksRead,
				userLabel, datnameLabel, queryidLabel,
			)

			sharedBlocksDirtied := 0.0
			if shared_blks_dirtied.Valid {
				sharedBlocksDirtied = float64(shared_blks_dirtied.Int64)
			}
			ch <- prometheus.MustNewConstMetric(
				statStatementsSharedBlocksDirtied,
				prometheus.CounterValue,
				sharedBlocksDirtied,
				userLabel, datnameLabel, queryidLabel,
			)

			sharedBlocksWritten := 0.0
			if shared_blks_written.Valid {
				sharedBlocksWritten = float64(shared_blks_written.Int64)
			}
			ch <- prometheus.MustNewConstMetric(
				statStatementsSharedBlocksWritten,
				prometheus.CounterValue,
				sharedBlocksWritten,
				userLabel, datnameLabel, queryidLabel,
			)

			localBlocksHits := 0.0
			if local_blks_hit.Valid {
				localBlocksHits = float64(local_blks_hit.Int64)
			}
			ch <- prometheus.MustNewConstMetric(
				statStatementsLocalBlocksHit,
				prometheus.CounterValue,
				localBlocksHits,
				userLabel, datnameLabel, queryidLabel,
			)

			localBlocksRead := 0.0
			if local_blks_read.Valid {
				localBlocksRead = float64(local_blks_read.Int64)
			}
			ch <- prometheus.MustNewConstMetric(
				statStatementsLocalBlocksRead,
				prometheus.CounterValue,
				localBlocksRead,
				userLabel, datnameLabel, queryidLabel,
			)

			localBlocksDirtied := 0.0
			if local_blks_dirtied.Valid {
				localBlocksDirtied = float64(local_blks_dirtied.Int64)
			}
			ch <- prometheus.MustNewConstMetric(
				statStatementsLocalBlocksDirtied,
				prometheus.CounterValue,
				localBlocksDirtied,
				userLabel, datnameLabel, queryidLabel,
			)

			localBlocksWritten := 0.0
			if local_blks_written.Valid {
				localBlocksWritten = float64(local_blks_written.Int64)
			}
			ch <- prometheus.MustNewConstMetric(
				statStatementsLocalBlocksWritten,
				prometheus.CounterValue,
				localBlocksWritten,
				userLabel, datnameLabel, queryidLabel,
			)

			tempBlocksRead := 0.0
			if temp_blks_read.Valid {
				tempBlocksRead = float64(temp_blks_read.Int64)
			}
			ch <- prometheus.MustNewConstMetric(
				statStatementsTempBlocksRead,
				prometheus.CounterValue,
				tempBlocksRead,
				userLabel, datnameLabel, queryidLabel,
			)

			tempBlocksWritten := 0.0
			if temp_blks_written.Valid {
				tempBlocksWritten = float64(temp_blks_written.Int64)
			}
			ch <- prometheus.MustNewConstMetric(
				statStatementsTempBlocksWritten,
				prometheus.CounterValue,
				tempBlocksWritten,
				userLabel, datnameLabel, queryidLabel,
			)

			walRecords := 0.0
			if wal_records.Valid {
				walRecords = float64(wal_records.Int64)
			}
			ch <- prometheus.MustNewConstMetric(
				statStatementsWalRecords,
				prometheus.CounterValue,
				walRecords,
				userLabel, datnameLabel, queryidLabel,
			)

			walFpi := 0.0
			if wal_fpi.Valid {
				walFpi = float64(wal_fpi.Int64)
			}
			ch <- prometheus.MustNewConstMetric(
				statStatementsWalFpi,
				prometheus.CounterValue,
				walFpi,
				userLabel, datnameLabel, queryidLabel,
			)
		}

		if c.includeQueryStatement {
			_, ok := presentQueryIds[queryidLabel]
			if !ok {
				presentQueryIds[queryidLabel] = struct{}{}

				queryLabel := "unknown"
				if statement.Valid {
					queryLabel = statement.String
				}

				ch <- prometheus.MustNewConstMetric(
					statStatementsQuery,
					prometheus.CounterValue,
					1,
					queryidLabel, queryLabel,
				)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func (c PGStatStatementsCollector) buildExclusionClause(identifiers []string, clauseTemplate string) string {
	if len(identifiers) == 0 {
		return ""
	}

	escaped := make([]string, 0, len(identifiers))
	for _, identifier := range identifiers {
		escaped = append(escaped, fmt.Sprintf("'%s'", strings.ReplaceAll(identifier, "'", "''")))
	}

	return fmt.Sprintf(clauseTemplate, strings.Join(escaped, ", "))
}
