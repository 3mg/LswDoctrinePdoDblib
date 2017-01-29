<?php

namespace Lsw\DoctrinePdoDblib\Doctrine\Platforms;
use Doctrine\DBAL\Platforms\SQLServer2008Platform as SQLServer;
use Doctrine\DBAL\Schema\TableDiff;

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
            $typeIndex = array_search('type', $columnDiff->changedProperties);
            $lengthIndex = array_search('length', $columnDiff->changedProperties);

            if ($typeIndex !== false || $lengthIndex !== false) {
                $fromColumn = $columnDiff->fromColumn;
                $toColumn = $columnDiff->column;
                $fromDBType = $fromColumn->getType()->getSQLDeclaration($fromColumn->toArray(), $this);
                $toDBType = $toColumn->getType()->getSQLDeclaration($fromColumn->toArray(), $this);

                if ($fromDBType == $toDBType) {
                    if ($typeIndex !== false) {
                        unset($columnDiff->changedProperties[$typeIndex]);
                    }
                    if ($lengthIndex !== false) {
                        unset($columnDiff->changedProperties[$lengthIndex]);
                    }
                }
            }

            if (count($columnDiff->changedProperties) == 0) {
                unset($diff->changedColumns[$columnName]);
            }
        }

        return parent::getAlterTableSQL($diff);
    }
}
