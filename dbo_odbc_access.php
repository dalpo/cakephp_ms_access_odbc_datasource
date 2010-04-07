<?php
/**
 * Cakephp MS Access datasource via ODBC
 * based on: http://bakery.cakephp.org/articles/view/a-cakephp-adodb-data-source-driver-for-ms-access
 *
 *
 * @package       cake
 * @subpackage    cake.cake.libs.model.datasources.dbo
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
    function limit($limit, $offset = null) {
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
    function renderStatement($type, $data) {
        switch (strtolower($type)) {
            case 'select':
                extract($data);
                $fields = trim($fields);

                if (strpos($limit, 'TOP') !== false && strpos($fields, 'DISTINCT ') === 0) {
                    $limit = 'DISTINCT ' . trim($limit);
                    $fields = substr($fields, 9);
                }
                return "SELECT {$limit} {$fields} FROM {$table} {$alias} {$joins} {$conditions} {$group} {$order}";
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
    function update(&$model, $fields = array(), $values = array()) {
        foreach ($fields as $i => $field) {
            if ($field == $model->primaryKey) {
                unset ($fields[$i]);
                unset ($values[$i]);
                break;
            }
        }
        return DboSource::update($model, $fields, $values);
    }

    function buildStatement($query, $model) {
        $join_parentheses = '';
        $query = array_merge(array('offset' => null, 'joins' => array()), $query);
        if (!empty($query['joins'])) {
            for ($i = 0; $i < count($query['joins']); $i++) {
                if (is_array($query['joins'][$i])) {
                    $query['joins'][$i] = $this->buildJoinStatement($query['joins'][$i]);
                    if ($i > 0) $join_parentheses = $join_parentheses . '(';
                }
            }
        }
        $join_parentheses = $join_parentheses . ' ';
        return $this->renderStatement('select', array(
                'conditions' => $this->conditions($query['conditions']),
                'fields' => join(', ', $query['fields']),
                'table' => $join_parentheses . $query['table'],
                'alias' => $this->alias . $this->name($query['alias']),
                'order' => $this->order($query['order']),
                'limit' => $this->limit($query['limit'], $query['offset']),
                'joins' => join(' ) ', $query['joins']),
                'group' => $this->group($query['group'])
        ));
    }

    function renderJoinStatement($data) {
        extract($data);
        if (empty($type)) {
            return trim("INNER JOIN {$table} {$alias} ON ({$conditions})");
        } else {
            return trim("{$type} JOIN {$table} {$alias} ON ({$conditions})");
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
    function execute($sql, $options = array()) {
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
    function fields(&$model, $alias = null, $fields = array(), $quote = true) {
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

        if ($count >= 1 && !in_array($fields[0], array('*', 'COUNT(*)'))) {
            for ($i = 0; $i < $count; $i++) {
                if (is_object($fields[$i]) && isset($fields[$i]->type) && $fields[$i]->type === 'expression') {
                    $fields[$i] = $fields[$i]->value;
                } elseif (preg_match('/^\(.*\)\s' . $this->alias . '.*/i', $fields[$i])) {
                    continue;
                } elseif (!preg_match('/^.+\\(.*\\)/', $fields[$i])) {
                    $prepend = '';

                    if (strpos($fields[$i], 'DISTINCT') !== false) {
                        $prepend = 'DISTINCT ';
                        $fields[$i] = trim(str_replace('DISTINCT', '', $fields[$i]));
                    }
                    $dot = strpos($fields[$i], '.');

                    if ($dot === false) {
                        $prefix = !(
                                strpos($fields[$i], ' ') !== false ||
                                        strpos($fields[$i], '(') !== false
                        );
                        $fields[$i] = $this->name(($prefix ? $alias . '.' : '') . $fields[$i]);
                    } else {
                        $value = array();
                        $comma = strpos($fields[$i], ',');
                        if ($comma === false) {
                            $build = explode('.', $fields[$i]);
                            if (!Set::numeric($build)) {
                                $fields[$i] = $this->name($build[0] . '.' . $build[1]);
                            }
                            $comma = String::tokenize($fields[$i]);
                            foreach ($comma as $string) {
                                if (preg_match('/^[0-9]+\.[0-9]+$/', $string)) {
                                    $value[] = $string;
                                } else {
                                    $build = explode('.', $string);
                                    $value[] = $this->name(trim($build[0]) . '.' . trim($build[1]));
                                }
                            }
                            $fields[$i] = implode(', ', $value);
                        }
                    }
                    $fields[$i] = $prepend . $fields[$i];
                } elseif (preg_match('/\(([\.\w]+)\)/', $fields[$i], $field)) {
                    if (isset($field[1])) {
                        if (strpos($field[1], '.') === false) {
                            $field[1] = $this->name($alias . '.' . $field[1]);
                        } else {
                            $field[0] = explode('.', $field[1]);
                            if (!Set::numeric($field[0])) {
                                $field[0] = implode('.', array_map(array($this, 'name'), $field[0]));
                                $fields[$i] = preg_replace('/\(' . $field[1] . '\)/', '(' . $field[0] . ')', $fields[$i], 1);
                            }
                        }
                    }
                }
            }
        }
        return array_unique($fields);
    }

}
?>