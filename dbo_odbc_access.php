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


}
?>