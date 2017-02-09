<?php

namespace Lsw\DoctrinePdoDblib\Doctrine\Platforms;

use Doctrine\DBAL\Platforms\SQLServer2008Platform as SQLServer;
use Doctrine\DBAL\Schema\TableDiff;
use Doctrine\DBAL\Schema\Column;

class SQLServer2008Platform extends SQLServer
{
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
}
