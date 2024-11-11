<?php

namespace Devsrealm\TonicsQueryBuilder;

class TonicsQueryBuilder
{
    private ?\PDO $pdo = null;

    private TonicsQuery $tonicsQuery;

    private Tables $tables;

    public function __construct(\PDO $PDO, TonicsQuery $tonicsQuery, Tables $tables)
    {
        $this->pdo = $PDO;
        $this->tonicsQuery = $tonicsQuery;
        $this->tonicsQuery->setTonicsQueryBuilder($this);
        $this->tables = $tables;
    }

    /**
     * @return \PDO
     */
    public function getPdo(): \PDO
    {
        if ($this->pdo === null){
            throw new \Exception("PDO Object has been destroyed, create a new one");
        }
        return $this->pdo;
    }

    public function destroyPdoConnection()
    {
        $this->pdo = null;

    }

    /**
     * @param \PDO $pdo
     * @return TonicsQueryBuilder
     */
    public function setPdo(\PDO $pdo): TonicsQueryBuilder
    {
        $this->pdo = $pdo;
        return $this;
    }

    /**
     * @return TonicsQuery
     */
    public function getTonicsQuery(): TonicsQuery
    {
        return $this->tonicsQuery;
    }

    public function getNewQuery(): TonicsQuery
    {
        $clone = clone $this->tonicsQuery;
        $clone->Reset();
        $clone->setTonicsQueryBuilder($this);
        return $clone;
    }

    /**
     * @param TonicsQuery $tonicsQuery
     * @return TonicsQueryBuilder
     */
    public function setTonicsQuery(TonicsQuery $tonicsQuery): TonicsQueryBuilder
    {
        $this->tonicsQuery = $tonicsQuery;
        return $this;
    }

    /**
     * @return Tables
     */
    public function getTables(): Tables
    {
        return $this->tables;
    }

    /**
     * @param Tables $tables
     */
    public function setTables(Tables $tables): void
    {
        $this->tables = $tables;
    }
}