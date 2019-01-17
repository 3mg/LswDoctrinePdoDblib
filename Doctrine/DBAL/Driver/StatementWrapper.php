<?php

namespace Lsw\DoctrinePdoDblib\Doctrine\DBAL\Driver;

/**
 */
class StatementWrapper {
    /** @var \PDOStatement */
    private $stmt;
    private $connection;
    private $driverOptions;

    private $boundParams = [];

    public function __construct($stmt, $connection, $driverOptions)
    {
        $this->stmt = $stmt;
        $this->connection = $connection;
        $this->driverOptions = $driverOptions;
    }

    public function __call($method, $arguments) {
        return call_user_func_array([$this->stmt, $method], $arguments);
    }

    function __get($name) {
        return $this->stmt->$name;
    }

    public function bindValue ($parameter, $value, $data_type = \PDO::PARAM_STR) {
        $this->boundParams[$parameter] = [$value, $data_type];

        return $this->stmt->bindValue($parameter, $value, $data_type);
    }

    public function execute ($input_parameters = null) {
        $stmt = $this->stmt;
        $params = $input_parameters ? $input_parameters : $this->boundParams;

        if (count($params) > 0 && is_int(array_keys($params)[0])) {
            preg_match_all('/<<(\d+)>>/', $this->stmt->queryString, $matches);

            if (count($matches[1]) > 0) {
                $newParams = [];
                $i = 1;

                for (; $i < $matches[1][0]; $i++) {
                    $newParams[] = $params[$i];
                }

                foreach ($matches[1] as $parameterNumber) {
                    $newParams[] = $params[intval($parameterNumber)];
                }

                for (; $i <= count($params); $i++) {
                    $newParams[] = $params[$i];
                }

                $stmt = $this->connection->prepareNonWrapped(
                    preg_replace('/<<\d+?>>/', '?', $this->stmt->queryString),
                    $this->driverOptions
                );

                for ($i = 0; $i < count($newParams); $i++) {
                    $stmt->bindValue($i + 1, $newParams[$i][0], $newParams[$i][1]);
                }

                if ($input_parameters) {
                    $input_parameters = $newParams;
                }

                $this->stmt = $stmt;
            }
        }

        return $stmt->execute($input_parameters);
    }


    public function bindParam(
        $parameter,
        &$variable,
        $data_type = \PDO::PARAM_STR,
        $length = null,
        $driver_options = null
    ) {
        return $this->stmt->bindParam(
            $parameter,
            $variable,
            $data_type,
            $length,
            $driver_options
        );
    }

    public function bindColumn($column, &$param, $type = null, $maxlen = null, $driverdata = null)
    {
        return $this->stmt->bindColumn($column, $param, $type, $maxlen, $driverdata);
    }

    /*public function fetch($fetch_style = null, $cursor_orientation = \PDO::FETCH_ORI_NEXT, $cursor_offset = 0)
    {
        return $this->stmt->fetch($fetch_style, $cursor_orientation, $cursor_offset);
    }

    public function rowCount()
    {
        return $this->stmt->rowCount();
    }

    public function fetchColumn($column_number = 0)
    {
        return $this->stmt->fetchColumn($column_number);
    }

    public function fetchAll($fetch_style = null, $fetch_argument = null, array $ctor_args = array())
    {
        return $this->stmt->fetchAll($fetch_style, $fetch_argument, $ctor_args);
    }

    public function fetchObject($class_name = "stdClass", array $ctor_args = array())
    {
        return $this->stmt->fetchObject($class_name, $ctor_args);
    }

    public function errorCode()
    {
        return $this->stmt->errorCode();
    }

    public function errorInfo()
    {
        return $this->stmt->errorInfo();
    }

    public function setAttribute($attribute, $value)
    {
        return $this->stmt->setAttribute($attribute, $value);
    }

    public function getAttribute($attribute)
    {
        return $this->stmt->getAttribute($attribute);
    }

    public function columnCount()
    {
        return $this->stmt->columnCount();
    }

    public function getColumnMeta($column)
    {
        return $this->stmt->getColumnMeta($column);
    }

    public function nextRowset()
    {
        return $this->stmt->nextRowset();
    }

    public function closeCursor()
    {
        return $this->stmt->closeCursor();
    }

    public function debugDumpParams()
    {
        return $this->stmt->debugDumpParams();
    }

    public function setFetchMode($fetchMode, $arg2 = null, $arg3 = null)
    {
        return $this->stmt->setFetchMode($fetchMode, $arg2, $arg3);
    }*/
}


