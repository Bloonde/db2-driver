<?php

namespace BWICompanies\DB2Driver;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar;

class DB2QueryGrammar extends Grammar
{

protected $tablePrefix = '';
    /**
     * The format for database stored dates.
     *
     * @var string
     */
    protected $dateFormat;

    /**
     * Offset compatibility mode true triggers FETCH FIRST X ROWS and ROW_NUM behavior for older versions of DB2
     *
     * @var bool
     */
    protected $offsetCompatibilityMode = true;

         /**
     * Forzar case-insensitive para LIKE / NOT LIKE envolviendo columna y valor en UPPER().
     * Respeta expresiones crudas (Expression) para no duplicar UPPER().
     *
     * @param Builder $query
     * @param array   $where
     * @return string
     */
        protected function whereBasic(Builder $query, $where)
    {
        $operator = strtoupper($where['operator']);

        if (in_array($operator, ['LIKE', 'NOT LIKE'])) {
            $column = $where['column'] instanceof Expression
                ? $this->getValue($where['column'])
                : $this->upperSafeColumn($where['column']);

            $value = $where['value'] instanceof Expression
                ? $this->getValue($where['value'])
                : 'UPPER('.$this->parameter($where['value']).')';

            return $column.' '.$operator.' '.$value;
        }

        return $this->wrap($where['column']).' '.$where['operator'].' '.$this->parameter($where['value']);
    }

    /**
     * Aplica UPPER directo salvo columnas que requieren CAST (fechas/horas).
     * Evita CHAR() sobre JSON/CLOB grandes que dispara SQL0137.
     *
     * @param string $column
     * @return string
     */
    protected function upperSafeColumn($column)
    {
        if ($this->needsCharCast($column)) {
            return 'UPPER(CHAR('.$this->wrap($column).'))';
        }
        return 'UPPER('.$this->wrap($column).')';
    }

    /**
     * Heurística simple para decidir cuándo castear (fechas/horas).
     * Personalizable ampliando el patrón.
     *
     * @param string $column
     * @return bool
     */
    protected function needsCharCast($column)
    {
        return (bool) preg_match('/(_at|_date|_time)$/i', $column);
    }

    /**
     * Optimización segura: convertir IN/NOT IN de un único valor en comparación directa.
     * No accede a clave 'not' (Laravel separa whereIn y whereNotIn) evitando errores de índice.
     */
    protected function whereIn(Builder $query, $where)
    {
        $values = $where['values'];

        if ($values instanceof Builder || $values instanceof \Closure) {
            return parent::whereIn($query, $where);
        }

        if (is_array($values)) {
            if (count($values) === 0) {
                return '0 = 1';
            }

            if (count($values) === 1) {
                $only = reset($values);
                if (!($only instanceof Expression)) {
                    $column = $this->wrap($where['column']);
                    $parameter = $this->parameter($only);
                    return $column.' = '.$parameter;
                }
            }
        }

        return parent::whereIn($query, $where);
    }

    protected function whereNotIn(Builder $query, $where)
    {
        $values = $where['values'];

        if ($values instanceof Builder || $values instanceof \Closure) {
            return parent::whereNotIn($query, $where);
        }

        if (is_array($values) && count($values) === 0) {
            // NOT IN () es tautología
            return '1 = 1';
        }

        if (is_array($values) && count($values) === 1 && !($values[0] instanceof Expression)) {
            $column = $this->wrap($where['column']);
            $parameter = $this->parameter($values[0]);
            return $column.' <> '.$parameter;
        }

        return parent::whereNotIn($query, $where);
    }

    /**
     * Wrap a single string in keyword identifiers.
     *
     * @param  string  $value
     * @return string
     */
    protected function wrapValue($value)
    {
        if ($value === '*') {
            return $value;
        }

        return str_replace('"', '""', $value);
    }

    /**
     * Compile the "limit" portions of the query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  int  $limit
     * @return string
     */
    protected function compileLimit(Builder $query, $limit)
    {
        if ($this->offsetCompatibilityMode) {
            return "FETCH FIRST $limit ROWS ONLY";
        }

        return parent::compileLimit($query, $limit);
    }

    /**
     * Compile a select query into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @return string
     */
    public function compileSelect(Builder $query)
    {
        if (! $this->offsetCompatibilityMode) {
            return parent::compileSelect($query);
        }

        if (is_null($query->columns)) {
            $query->columns = ['*'];
        }

        $components = $this->compileComponents($query);

        // If an offset is present on the query, we will need to wrap the query in
        // a big "ANSI" offset syntax block. This is very nasty compared to the
        // other database systems but is necessary for implementing features.
        if ($query->offset > 0) {
            return $this->compileAnsiOffset($query, $components);
        }

        return $this->concatenate($components);
    }

    /**
     * Create a full ANSI offset clause for the query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $components
     * @return string
     */
    protected function compileAnsiOffset(Builder $query, $components)
    {
        // An ORDER BY clause is required to make this offset query work, so if one does
        // not exist we'll just create a dummy clause to trick the database and so it
        // does not complain about the queries for not having an "order by" clause.
        if (! isset($components['orders'])) {
            $components['orders'] = 'order by 1';
        }

        unset($components['limit']);

        // We need to add the row number to the query so we can compare it to the offset
        // and limit values given for the statements. So we will add an expression to
        // the "select" that will give back the row numbers on each of the records.
        $orderings = $components['orders'];

        $columns = (! empty($components['columns']) ? $components['columns'].', ' : 'select');

        if ($columns == 'select *, ' && $query->from) {
            $columns = 'select '.$this->tablePrefix.$query->from.'.*, ';
        }

        $components['columns'] = $this->compileOver($orderings, $columns);

        // if there are bindings in the order, we need to move them to the select since we are moving the parameter
        // markers there with the OVER statement
        if (isset($query->getRawBindings()['order'])) {
            $query->addBinding($query->getRawBindings()['order'], 'select');
            $query->setBindings([], 'order');
        }

        unset($components['orders']);

        // Next we need to calculate the constraints that should be placed on the query
        // to get the right offset and limit from our query but if there is no limit
        // set we will just handle the offset only since that is all that matters.
        $start = $query->offset + 1;

        $constraint = $this->compileRowConstraint($query);

        $sql = $this->concatenate($components);

        // We are now ready to build the final SQL query so we'll create a common table
        // expression from the query and get the records with row numbers within our
        // given limit and offset value that we just put on as a query constraint.
        return $this->compileTableExpression($sql, $constraint);
    }

    /**
     * Compile the over statement for a table expression.
     *
     * @param  string  $orderings
     * @param    $columns
     * @return string
     */
    protected function compileOver($orderings, $columns)
    {
        return "{$columns} row_number() over ({$orderings}) as row_num";
    }

    /**
     * @param $query
     * @return string
     */
    protected function compileRowConstraint($query)
    {
        $start = $query->offset + 1;

        if ($query->limit > 0) {
            $finish = $query->offset + $query->limit;

            return "between {$start} and {$finish}";
        }

        return ">= {$start}";
    }

    /**
     * Compile a common table expression for a query.
     *
     * @param  string  $sql
     * @param  string  $constraint
     * @return string
     */
    protected function compileTableExpression($sql, $constraint)
    {
        return "select * from ({$sql}) as temp_table where row_num {$constraint}";
    }

    /**
     * Compile the "offset" portions of the query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  int  $offset
     * @return string
     */
    protected function compileOffset(Builder $query, $offset)
    {
        if ($this->offsetCompatibilityMode) {
            return '';
        }

        return parent::compileOffset($query, $offset);
    }

    /**
     * Compile an exists statement into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @return string
     */
    public function compileExists(Builder $query)
    {
        $existsQuery = clone $query;

        $existsQuery->columns = [];

        return $this->compileSelect($existsQuery->selectRaw('1 exists')->limit(1));
    }

    /**
     * Get the format for database stored dates.
     *
     * @return string
     */
    public function getDateFormat()
    {
        return $this->dateFormat ?? parent::getDateFormat();
    }

    /**
     * Set the format for database stored dates.
     *
     * @param $dateFormat
     */
    public function setDateFormat($dateFormat)
    {
        $this->dateFormat = $dateFormat;
    }

    /**
     * Set offset compatibility mode to trigger FETCH FIRST X ROWS and ROW_NUM behavior for older versions of DB2
     *
     * @param $bool
     */
    public function setOffsetCompatibilityMode($bool)
    {
        $this->offsetCompatibilityMode = $bool;
    }

    /**
     * Compile the SQL statement to define a savepoint.
     *
     * @param  string  $name
     * @return string
     */
    public function compileSavepoint($name)
    {
        return 'SAVEPOINT '.$name.' ON ROLLBACK RETAIN CURSORS';
    }
}
