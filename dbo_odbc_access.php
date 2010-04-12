<?php
/**
 * Cakephp MS Access datasource via ODBC
 * based on: http://bakery.cakephp.org/articles/view/a-cakephp-adodb-data-source-driver-for-ms-access
 *
 *
 * @package       cake
 * @subpackage    cake.cake.libs.model.datasources.dbo
 * @author        Andrea Dal Ponte - dalpo85@gmail.com
 * @link          http://github.com/dalpo/cakephp_ms_access_odbc_datasource
 */
App::import('Core', 'DboOdbc');
class DboOdbcAccess extends DboOdbc {
    /**
     * Driver description
     *
     * @var string
     */
    public $description = "ODBC DBO Driver for MS Access";

    /**
     * Returns a limit statement in the correct format for the particular database.
     *
     * @param integer $limit Limit of results returned
     * @param integer $offset Offset from which to start results
     * @return string SQL limit/offset statement
     */
    public function limit($limit, $offset = null) {
        if ($limit) {
            $rt = '';
            if (!strpos(strtolower($limit), 'top') || strpos(strtolower($limit), 'top') === 0) {
                $rt = ' TOP';
            }
            $rt .= ' ' . $limit;
            if (is_int($offset) && $offset > 0) {
                $rt .= ' OFFSET ' . $offset;
            }
            return $rt;
        }
        return null;
    }

    /**
     * Builds final SQL statement
     *
     * @param string $type Query type
     * @param array $data Query data
     * @return string
     */
    public function renderStatement($type, $data) {
        switch (strtolower($type)) {
            case 'select':
                extract($data);
                $fields = trim($fields);

                if (strpos($limit, 'TOP') !== false && strpos($fields, 'DISTINCT ') === 0) {
                    $limit = 'DISTINCT ' . trim($limit);
                    $fields = substr($fields, 9);
                }

                $fromStatement = "{$table} {$alias}";
                foreach ($joins as $join) {
                    $fromStatement = " ( ".$fromStatement." ) ".$join;
                }

                return "SELECT {$limit} {$fields} FROM {$fromStatement} {$conditions} {$group} {$order}";
                break;
            default:
                return DboSource::renderStatement($type, $data);
                break;
        }
    }

    /**
     * Removes Identity (primary key) column from update data before returning to parent
     *
     * @param Model $model
     * @param array $fields
     * @param array $values
     * @return array
     */
    public function update(&$model, $fields = array(), $values = array()) {
        foreach ($fields as $i => $field) {
            if ($field == $model->primaryKey) {
                unset ($fields[$i]);
                unset ($values[$i]);
                break;
            }
        }
        return DboSource::update($model, $fields, $values);
    }

    /**
     * Builds and generates an SQL statement from an array.	 Handles final clean-up before conversion.
     *
     * @param array $query An array defining an SQL query
     * @param object $model The model object which initiated the query
     * @return string An executable SQL statement
     * @see DboSource::renderStatement()
     */
    public function buildStatement($query, $model) {
        $query = array_merge(array('offset' => null, 'joins' => array()), $query);
        if (!empty($query['joins'])) {
            $count = count($query['joins']);
            for ($i = 0; $i < $count; $i++) {
                if (is_array($query['joins'][$i])) {
                    $query['joins'][$i] = $this->buildJoinStatement($query['joins'][$i]);
                }
            }
        }
        return $this->renderStatement('select', array(
                'conditions' => $this->conditions($query['conditions'], true, true, $model),
                'fields' => implode(', ', $query['fields']),
                'table' => $query['table'],
                'alias' => $this->alias . $this->name($query['alias']),
                'order' => $this->order($query['order']),
                'limit' => $this->limit($query['limit'], $query['offset']),
                'joins' => $query['joins'],
                'group' => $this->group($query['group'])
        ));
    }

    /**
     * Builds and generates a JOIN statement from an array.	 Handles final clean-up before conversion.
     *
     * @param array $join An array defining a JOIN statement in a query
     * @return string An SQL JOIN statement to be used in a query
     * @see DboSource::renderJoinStatement()
     * @see DboSource::buildStatement()
     */
    function buildJoinStatement($join) {
        $data = array_merge(array(
                'type' => null,
                'alias' => null,
                'table' => 'join_table',
                'conditions' => array()
                ), $join);

        if (!empty($data['alias'])) {
            $data['alias'] = $this->alias . $this->name($data['alias']);
        }
        if (!empty($data['conditions'])) {
            $data['conditions'] = trim($this->conditions($data['conditions'], true, false));
        }
        $data['table'] = $this->name($data['table']);
        return $this->renderJoinStatement($data);
    }

    /**
     * Renders a final SQL JOIN statement
     *
     * @param array $data
     * @return string
     */
    public function renderJoinStatement($data) {
        extract($data);
        if (empty($type)) {
            return "INNER JOIN {$table} {$alias} ON ({$conditions})";
        } else {
            return "{$type} JOIN {$table} {$alias} ON ({$conditions})";
        }
    }

    /**
     * Queries the database with given SQL statement, and obtains some metadata about the result
     * (rows affected, timing, any errors, number of rows in resultset). The query is also logged.
     * If DEBUG is set, the log is shown all the time, else it is only shown on errors.
     *
     * @param string $sql
     * @param array $options
     * @return mixed Resource or object representing the result set, or false on failure
     */
    public function execute($sql, $options = array()) {
        $defaults = array('stats' => true, 'log' => $this->fullDebug);
        $options = array_merge($defaults, $options);

        $t = getMicrotime();
        $result = $this->_result = $this->_execute($sql);
        $this->error = null;

        if ($options['stats']) {
            $this->took = round((getMicrotime() - $t) * 1000, 0);
            if(!$result) {
                $this->error = $this->lastError();
            }
            $this->affected = $this->lastAffected();
            $this->numRows = $this->lastNumRows();
        }

        if ($options['log']) {
            $this->logQuery($sql);
        }

        if ($this->error) {
            $this->showQuery($sql);
            return false;
        }
        return $this->_result;
    }



    /**
     * Generates the fields list of an SQL query.
     *
     * @param Model $model
     * @param string $alias Alias tablename
     * @param mixed $fields
     * @param boolean $quote If false, returns fields array unquoted
     * @return array
     */
    public function fields(&$model, $alias = null, $fields = array(), $quote = true) {
        if (empty($alias)) {
            $alias = $model->alias;
        }
        if (empty($fields)) {
            $fields = array_keys($model->schema());
        } elseif (!is_array($fields)) {
            $fields = String::tokenize($fields);
        }
        $fields = array_values(array_filter($fields));

        if (!$quote) {
            return $fields;
        }
        $count = count($fields);
        $resultFields = array();

        foreach ($fields as $field_item) {

            if (is_object($field_item) && isset($field_item->type) && $field_item->type === 'expression') {
                $resultFields[] = $field_item->value;
            } elseif (preg_match('/^\(.*\)\s' . $this->alias . '.*/i', $field_item) || strrpos($field_item, '_dot_') || strrpos($field_item, ' AS ')) {
                $resultFields[] = $field_item;
//            } elseif (strrpos($field_item, 'COUNT')) {
//                debug($field_item);
//                die();
//                $resultFields[] = $field_item;
            } elseif(strrpos($field_item, '*') !== false) {
                /**
                 * workaround
                 */
                $builds = explode('.', $field_item);
                $newFieldList = array();

                if( count($builds) == 1 ) {
                    continue;
                } else {
                    if($builds[0] != $model->alias) {
                        $model = $this->_getModel($builds[0]);
                        $alias = $builds[0];
                    }
                }

                $describeList = $this->describe($model);
                foreach ($describeList as $describeItemKey => $describeItemName) {
                    $newFieldList[] = "{$alias}.{$describeItemKey}";
                }

                $resultFields = array_merge($resultFields, $this->fields($model, $alias, $newFieldList, $quote));


            } elseif (!preg_match('/^.+\\(.*\\)/', $field_item)) {
                $prepend = '';

                if (strpos($field_item, 'DISTINCT') !== false) {
                    $prepend = 'DISTINCT ';
                    $field_item = trim(str_replace('DISTINCT', '', $field_item));
                }
                $dot = strpos($field_item, '.');

                if ($dot === false) {
                    $prefix = !(
                            strpos($field_item, ' ') !== false ||
                                    strpos($field_item, '(') !== false
                    );
                    $field_item = $this->name(($prefix ? $alias . '.' : '') . $field_item) . ' AS ' . $this->name($alias . '_dot_' . $field_item);
                } else {
                    $value = array();
                    $comma = strpos($field_item, ',');
                    if ($comma === false) {
                        $build = explode('.', $field_item);
                        $field_item = $this->name($build[0] . '.' . $build[1]) . ' AS ' . $this->name($build[0] . '_dot_' . $build[1]);
                    }
                }
                $resultFields[] = $prepend . $field_item;
            } elseif (preg_match('/\(([\.\w]+)\)/', $field_item, $field)) {
                if (isset($field[1])) {
                    if (strpos($field[1], '.') === false) {
                        $resultFields[] = $this->name($alias . '.' . $field[1]);
                    } else {
                        $field[0] = explode('.', $field[1]);
                        if (!Set::numeric($field[0])) {
                            $field[0] = implode('.', array_map(array($this, 'name'), $field[0]));
                            $resultFields[] = preg_replace('/\(' . $field[1] . '\)/', '(' . $field[0] . ')', $field_item, 1);
                        }
                    }
                }
            }
        }

        return array_unique($resultFields);
    }

    /**
     * Fetches the next row from the current result set
     *
     * @return unknown
     */
    public function fetchResult() {
        if ($row = odbc_fetch_row($this->results)) {
            $resultRow = array();
            $numFields = odbc_num_fields($this->results);
            $i = 0;
            for($i = 0; $i < $numFields; $i++) {
                list($table, $column) = $this->map[$i];
//                $resultRow[$table][$column] = odbc_result($this->results, $i + 1);
                $resultRow[$table][$column] = @odbc_result($this->results, "{$table}_dot_{$column}");
            }
            return $resultRow;
        }
        return false;
    }


    /**
     * Returns an SQL calculation, i.e. COUNT() or MAX()
     *
     * @param model $model
     * @param string $func Lowercase name of SQL function, i.e. 'count' or 'max'
     * @param array $params Function parameters (any values must be quoted manually)
     * @return string An SQL calculation function
     * @access public
     */
    public function calculate(&$model, $func, $params = array()) {
        $params = (array)$params;

        switch (strtolower($func)) {
            case 'count':
                if (!isset($params[0])) {
                    $params[0] = '*';
                }
                if (!isset($params[1])) {
                    $params[1] = 'count';
                }
                return 'COUNT(' . $this->name($params[0]) . ') AS ' . $this->name($model->alias."_dot_".$params[1]);
            case 'max':
            case 'min':
                if (!isset($params[1])) {
                    $params[1] = $params[0];
                }
                return strtoupper($func) . '(' . $this->name($params[0]) . ') AS ' . $this->name($model->alias."_dot_".$params[1]);
                break;
        }
    }


    /**
     * Retrive a model instance
     *
     * @param string $name
     * @return object
     */
    protected function _getModel($name = null) {
        $model = null;
        $model = ClassRegistry::init($name);
        if( $name && !empty( $model ) ) {
            return $model;
        } else {
            trigger_error(__( __CLASS__."::_getModel() - Model is not set or could not be found", true ), E_USER_WARNING);
            return null;
        }
    }

}
?>