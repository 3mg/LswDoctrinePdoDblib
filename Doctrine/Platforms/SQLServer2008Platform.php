<?php

namespace Lsw\DoctrinePdoDblib\Doctrine\Platforms;

use Doctrine\DBAL\LockMode;
use Doctrine\DBAL\Platforms\SQLServer2008Platform as SQLServer;
use Doctrine\DBAL\Schema\TableDiff;

class SQLServer2008Platform extends SQLServer
{
    /**
     * Adds ability to override lock hints from symfony config by using 'platform_service' option
     *
     * @var array
     */
    protected $lockHints = array(
        LockMode::NONE              => ' WITH (NOLOCK)',
        LockMode::PESSIMISTIC_READ  => ' WITH (HOLDLOCK, ROWLOCK)',
        LockMode::PESSIMISTIC_WRITE => ' WITH (UPDLOCK, ROWLOCK)',
    );

    /**
     * @param array $lockHints
     */
    public function setLockHints($lockHints)
    {
        $this->lockHints = $lockHints;
    }

    /**
     * @param $lockMode
     * @param $hint
     */
    public function setLockHint($lockMode, $hint)
    {
        $this->lockHints[$lockMode] = $hint;
    }

    /**
     * {@inheritDoc}
     */
    public function getClobTypeDeclarationSQL(array $field)
    {
        return 'NVARCHAR(MAX)';
    }

    /**
     * {@inheritDoc}
     */
    public function getAlterTableSQL(TableDiff $diff)
    {
        $sql = array();
        $columnNames = array_keys($diff->changedColumns);

        foreach ($columnNames as $columnName) {
            /* @var $columnDiff \Doctrine\DBAL\Schema\ColumnDiff */
            $columnDiff = &$diff->changedColumns[$columnName];

            // Ignore 'unsigned' change as MSSQL don't support unsigned
            $unsignedIndex = array_search('unsigned', $columnDiff->changedProperties);

            if ($unsignedIndex !== false) {
                unset($columnDiff->changedProperties[$unsignedIndex]);
            }

            // As there is no property type hint for MSSQL, ignore type change if DB-Types are equal
            $props = array('type', 'length'/*, 'default'*/);
            $changedPropIndexes = array();

            foreach ($props as $prop) {
                if (($idx = array_search($prop, $columnDiff->changedProperties)) !== false) {
                    $changedPropIndexes[] = $idx;
                }
            }

            if (count($changedPropIndexes) > 0) {
                $fromColumn = $columnDiff->fromColumn;
                $toColumn = $columnDiff->column;
                $fromDBType = $fromColumn->getType()->getSQLDeclaration($fromColumn->toArray(), $this);
                $toDBType = $toColumn->getType()->getSQLDeclaration($fromColumn->toArray(), $this);

                if ($fromDBType == $toDBType) {
                    foreach ($changedPropIndexes as $index) {
                        unset($columnDiff->changedProperties[$index]);
                    }
                }
            }

            if (count($columnDiff->changedProperties) == 0) {
                unset($diff->changedColumns[$columnName]);
            }
        }

        // Original SQLServerPlatform tries to add default constraint
        // in separate query after columns created. For not-null columns it's fail
        /** @var \Doctrine\DBAL\Schema\Column $column */
        foreach (array_keys($diff->addedColumns) as $key) {
            $column = $diff->addedColumns[$key];

            if ($this->onSchemaAlterTableAddColumn($column, $diff, $columnSql)) {
                continue;
            }

            $columnDef = $column->toArray();

            $query = 'ALTER TABLE '.$diff->name.
                ' ADD '.$this->getColumnDeclarationSQL($column->getQuotedName($this), $columnDef);

            if (isset($columnDef['default'])) {
                $query .= ' CONSTRAINT ' .
                    $this->generateDefaultConstraintName($diff->name, $column->getName()) .
                    $this->getDefaultValueDeclarationSQL($columnDef);
            }

            $sql[] = $query;
            unset($diff->addedColumns[$key]);
        }

        // In original SQLServerPlatform, default constraint deletion missed
        // Also generateDefaultConstraintName and generateIdentifierName
        // are private, so redecleared them in this class
        foreach ($diff->removedColumns as $column) {
            if ($column->getDefault() !== null) {
                /**
                 * Drop existing column default constraint
                 */
                $constraintName = $this->generateDefaultConstraintName($diff->name, $column->getName());
                $sql[] =
                    'IF EXISTS(SELECT 1 FROM sys.objects WHERE type_desc = \'DEFAULT_CONSTRAINT\' AND name = \''.$constraintName.'\')'.
                    ' BEGIN '.
                    '  ALTER TABLE '.$diff->name.' DROP CONSTRAINT '.$constraintName.'; '.
                    ' END ';
            }
        }

        return array_merge($sql, parent::getAlterTableSQL($diff));
    }

    /**
     * {@inheritDoc}
     */
    public function appendLockHint($fromClause, $lockMode)
    {
        if (isset($this->lockHints[$lockMode])) {
            return $fromClause.$this->lockHints[$lockMode];
        }

        return $fromClause;
    }

    /**
     * {@inheritDoc}
     */
    protected function initializeDoctrineTypeMappings()
    {
        parent::initializeDoctrineTypeMappings();

        $this->doctrineTypeMapping['hierarchyid'] = 'blob';
    }

    /**
     * {@inheritDoc}
     */
    protected function getVarcharTypeDeclarationSQLSnippet($length, $fixed)
    {
        $length = is_numeric($length) ? $length * 2 : $length;
        if ($length > $this->getVarcharMaxLength() || $length < 0) {
            $length = 'MAX';
        }

        return $fixed ? ($length ? 'NCHAR(' . $length . ')' : 'NCHAR(255)') : ($length ? 'NVARCHAR(' . $length . ')' : 'NVARCHAR(255)');
    }


    /**
     * {@inheritDoc}
     */
    public function getListTableColumnsSQL($table, $database = null)
    {
        return "SELECT    col.name,
                          type.name AS type,
                          IIF(type.name IN ('nvarchar', 'nchar') AND col.max_length > 0, col.max_length / 2, col.max_length) AS length,
                          ~col.is_nullable AS notnull,
                          def.definition AS [default],
                          col.scale,
                          col.precision,
                          col.is_identity AS autoincrement,
                          col.collation_name AS collation,
                          CAST(prop.value AS NVARCHAR(MAX)) AS comment -- CAST avoids driver error for sql_variant type
                FROM      sys.columns AS col
                JOIN      sys.types AS type
                ON        col.user_type_id = type.user_type_id
                JOIN      sys.objects AS obj
                ON        col.object_id = obj.object_id
                JOIN      sys.schemas AS scm
                ON        obj.schema_id = scm.schema_id
                LEFT JOIN sys.default_constraints def
                ON        col.default_object_id = def.object_id
                AND       col.object_id = def.parent_object_id
                LEFT JOIN sys.extended_properties AS prop
                ON        obj.object_id = prop.major_id
                AND       col.column_id = prop.minor_id
                AND       prop.name = 'MS_Description'
                WHERE     obj.type = 'U'
                AND       " . $this->getTableWhereClause($table, 'scm.name', 'obj.name');
    }

    /**
     * @param string $table
     * @param string $column
     * @return string
     */
    protected function generateDefaultConstraintName($table, $column)
    {
        return 'DF_' . $this->generateIdentifierName($table) . '_' . $this->generateIdentifierName($column);
    }

    /**
     * Returns a hash value for a given identifier.
     *
     * @param string $identifier Identifier to generate a hash value for.
     *
     * @return string
     */
    protected function generateIdentifierName($identifier)
    {
        return strtoupper(dechex(crc32($identifier)));
    }


    /**
     * {@inheritDoc}
     */
    protected function doModifyLimitQuery($query, $limit, $offset = null)
    {
        if ($limit === null) {
            return $query;
        }

        $origQuery = $query;

        $start   = $offset + 1;
        $end     = $offset + $limit;
        $orderBy = stristr($query, 'ORDER BY');
        $query   = preg_replace('/\s+ORDER\s+BY\s+([^\)]*)/', '', $query); //Remove ORDER BY from $query
        $format  = 'SELECT * FROM (%s) AS doctrine_tbl WHERE doctrine_rownum BETWEEN %d AND %d';

        // Pattern to match "main" SELECT ... FROM clause (including nested parentheses in select list).
        $selectFromPattern = '/^(\s*SELECT\s+(?:\((?>[^()]+)|(?:R)*\)|[^(])+)\sFROM\s/i';

        if ( ! $orderBy) {
            //Replace only "main" FROM with OVER to prevent changing FROM also in subqueries.
            $query = preg_replace(
                $selectFromPattern,
                '$1, ROW_NUMBER() OVER (ORDER BY (SELECT 0)) AS doctrine_rownum FROM ',
                $query,
                1
            );

            return sprintf($format, $query, $start, $end);
        }

        //Clear ORDER BY
        $orderBy        = preg_replace('/ORDER\s+BY\s+([^\)]*)(.*)/', '$1', $orderBy);
        $orderByParts   = explode(',', $orderBy);
        $orderbyColumns = array();

        //Split ORDER BY into parts
        foreach ($orderByParts as &$part) {

            if (preg_match('/(([^\s]*)\.)?([^\.\s]*)\s*(ASC|DESC)?/i', trim($part), $matches)) {
                $orderbyColumns[] = array(
                    'column'    => $matches[3],
                    'hasTable'  => ( ! empty($matches[2])),
                    'sort'      => isset($matches[4]) ? $matches[4] : null,
                    'table'     => empty($matches[2]) ? '[^\.\s]*' : $matches[2]
                );
            }
        }

        //Find alias for each colum used in ORDER BY
        if ( ! empty($orderbyColumns)) {
            foreach ($orderbyColumns as $column) {

                $pattern    = sprintf('/%s\.(%s)\s*(AS)?\s*([^,\s\)]*)/i', $column['table'], $column['column']);
                $overColumn = preg_match($pattern, $query, $matches)
                    ? ($column['hasTable'] ? $column['table']  . '.' : '') . $column['column']
                    : $column['column'];

                //Replace dynamic column names with expressions
                if (!$column['hasTable']) {
                    preg_match('/(AS .+?\s*,)\s*(.+) AS '.$column['column'].'/', $query, $fieldExpression);

                    if (!$fieldExpression) {
                        preg_match('/(|SELECT\s*?)(.+) AS '.$column['column'].'/', $query, $fieldExpression);
                    }

                    if ($fieldExpression) {
                        $overColumn = $fieldExpression[2];

                        // Mark parameters to duplicate
                        if ($pos = strpos($overColumn, '?') !== false) {
                            $selectStatementPosition = strpos($origQuery, $overColumn);
                            $parameterNumber = substr_count($origQuery , '?', 0, $selectStatementPosition) + 1;

                            $parts = preg_split('/\?/', $overColumn);
                            $overColumn = '';

                            for ($i = 0; $i < count($parts) - 1; $i++) {
                                $overColumn .= $parts[$i].'<<'.($parameterNumber++).'>>';
                            }

                            $overColumn .= $parts[count($parts) - 1];
                        }
                    }
                }

                if (isset($column['sort'])) {
                    $overColumn .= ' ' . $column['sort'];
                }

                $overColumns[] = $overColumn;
            }
        }

        //Replace only first occurrence of FROM with $over to prevent changing FROM also in subqueries.
        $over  = 'ORDER BY ' . implode(', ', $overColumns);
        $query = preg_replace($selectFromPattern, "$1, ROW_NUMBER() OVER ($over) AS doctrine_rownum FROM ", $query, 1);

        return sprintf($format, $query, $start, $end);
    }

    /**
     * Returns the where clause to filter schema and table name in a query.
     *
     * @param string $table        The full qualified name of the table.
     * @param string $schemaColumn The name of the column to compare the schema to in the where clause.
     * @param string $tableColumn  The name of the column to compare the table to in the where clause.
     *
     * @return string
     */
    private function getTableWhereClause($table, $schemaColumn, $tableColumn)
    {
        if (strpos($table, '.') !== false) {
            [$schema, $table] = explode('.', $table);
            $schema           = $this->quoteStringLiteral($schema);
            $table            = $this->quoteStringLiteral($table);
        } else {
            $schema = 'SCHEMA_NAME()';
            $table  = $this->quoteStringLiteral($table);
        }

        return sprintf('(%s = %s AND %s = %s)', $tableColumn, $table, $schemaColumn, $schema);
    }
}


