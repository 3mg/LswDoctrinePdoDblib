<?php

namespace Lsw\DoctrinePdoDblib\Doctrine\Platforms;
use Doctrine\DBAL\Platforms\SQLServer2008Platform as SQLServer;
use Doctrine\DBAL\Schema\TableDiff;

class SQLServer2008Platform extends SQLServer
{
    /**
     * @var string
     */
    protected $dateTimeFormatString = 'Y-m-d H:i:s';
  
    /**
     * @return string
     */
    public function getDateTimeFormatString()
    {
        return $this->dateTimeFormatString;
    }
    
    /**
     * @param string $dateTimeFormatString
     * @return \Lsw\DoctrinePdoDblib\Doctrine\Platforms\SQLServer2008Platform
     */
    public function setDateTimeFormatString($dateTimeFormatString){
        $this->dateTimeFormatString = $dateTimeFormatString;
        return $this;
    }
    
    /**
     * {@inheritDoc}
     */
    public function getDateTimeTzFormatString()
    {
        return $this->getDateTimeFormatString();
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

            if ($typeIndex !== false) {
                $fromColumn = $columnDiff->fromColumn;
                $toColumn = $columnDiff->column;
                $fromDBType = $fromColumn->getType()->getSQLDeclaration($fromColumn->toArray(), $this);
                $toDBType = $toColumn->getType()->getSQLDeclaration($fromColumn->toArray(), $this);

                if ($fromDBType == $toDBType) {
                    unset($columnDiff->changedProperties[$typeIndex]);
                }
            }

            if (count($columnDiff->changedProperties) == 0) {
                unset($diff->changedColumns[$columnName]);
            }
        }

        return parent::getAlterTableSQL($diff);
    }
}
