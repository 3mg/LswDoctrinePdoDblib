<?php

namespace Lsw\DoctrinePdoDblib\Doctrine\DBAL\Driver\PDOODBC;

use Doctrine\DBAL\Driver\PDOStatement;

/**
 * PDOStatement extension for closing cursor after execute as ODBC expects
 */
class Statement extends PDOStatement implements \Doctrine\DBAL\Driver\Statement
{
    /**
     * Protected constructor.
     */
    protected function __construct()
    {
    }

    /**s
     * {@inheritdoc}
     */
    public function execute($params = null)
    {
        $result = parent::execute($params);
        $this->closeCursor();

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function bindValue($name, $value, $type = null)
    {
        return parent::bindValue($name, $value, $type == \PDO::PARAM_BOOL ? \PDO::PARAM_INT : $type);
    }
}
