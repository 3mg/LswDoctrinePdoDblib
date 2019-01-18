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
}


