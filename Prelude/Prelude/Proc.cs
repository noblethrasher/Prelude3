using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Collections;

namespace Prelude
{
    public sealed class SchemaAttribute : Attribute
    {
        public string Name { get; }

        public SchemaAttribute(string name) => Name = name;
    }

    public abstract class DbReaderProxyFactory<BaseReader, EnhancedReader>
        where BaseReader : IDataReader
        where EnhancedReader : IDataReader
    {
        public abstract EnhancedReader CreateReader(BaseReader rdr);
    }

    public interface CommandSubset<T>
        where T : IDataReader
    {
        T ExecuteReader();
        int ExecuteNonQuery();
    }

    public interface CommandEx<T> : IEnumerable<T>, CommandSubset<T>
        where T : IDataReader { }



    /// <summary>
    /// This is the base class for
    /// </summary>
    /// <typeparam name="DbParameter">The base type for query parameters for parameterized queries</typeparam>
    /// <typeparam name="DbConnection">The base type for the database connection</typeparam>
    /// <typeparam name="DbCommand">The base type for the ADO command</typeparam>
    /// <typeparam name="DbProxyFactory"></typeparam>
    /// <typeparam name="BaseReader">The base type for the ADO database reader</typeparam>
    /// <typeparam name="EnhancedReader"></typeparam>
    /// <typeparam name="Result">The base type for for the query result. This will usually be an IEnumerable<typeparamref name="T"/></typeparam>
    public abstract partial class Proc<DbParameter, DbConnection, DbCommand, DbProxyFactory, BaseReader, EnhancedReader, Result>
    {
        protected abstract class MaybeManagedCommand : IDbCommand
        {
            public DbCommand WrappedCommand { get; }

            public MaybeManagedConnection ManagedConnection { get; }

            public MaybeManagedCommand(DbCommand cmd, MaybeManagedConnection conn)
            {
                ManagedConnection = conn;
                WrappedCommand = cmd;
            }

            public IDbConnection Connection { get => WrappedCommand.Connection; set => WrappedCommand.Connection = value; }
            public IDbTransaction Transaction { get => WrappedCommand.Transaction; set => WrappedCommand.Transaction = value; }
            public string CommandText { get => WrappedCommand.CommandText; set => WrappedCommand.CommandText = value; }
            public int CommandTimeout { get => WrappedCommand.CommandTimeout; set => WrappedCommand.CommandTimeout = value; }
            public CommandType CommandType { get => WrappedCommand.CommandType; set => WrappedCommand.CommandType = value; }

            public IDataParameterCollection Parameters => WrappedCommand.Parameters;

            public UpdateRowSource UpdatedRowSource { get => WrappedCommand.UpdatedRowSource; set => WrappedCommand.UpdatedRowSource = value; }

            public void Cancel() => WrappedCommand.Cancel();

            public IDbDataParameter CreateParameter() => WrappedCommand.CreateParameter();

            public void Dispose() => WrappedCommand.Dispose();

            public int ExecuteNonQuery() => WrappedCommand.ExecuteNonQuery();

            public IDataReader ExecuteReader() => WrappedCommand.ExecuteReader();

            public IDataReader ExecuteReader(CommandBehavior behavior) => WrappedCommand.ExecuteReader(behavior);

            public object ExecuteScalar() => WrappedCommand.ExecuteScalar();

            public void Prepare() => WrappedCommand.Prepare();

            public abstract MaybeManagedDataReader GetManagedReader();
        }

        protected sealed class ManagedCommand : MaybeManagedCommand
        {
            public ManagedCommand(DbCommand cmd, MaybeManagedConnection conn) : base(cmd, conn) { }

            public override MaybeManagedDataReader GetManagedReader()
            {
                ManagedConnection.Open();

                return new ManagedDataReader((BaseReader)this.ExecuteReader(), ManagedConnection);
            }
        }

        protected sealed class UnmanagedCommand : MaybeManagedCommand
        {
            public UnmanagedCommand(DbCommand cmd, MaybeManagedConnection conn) : base(cmd, conn) { }

            public override MaybeManagedDataReader GetManagedReader() => new UnmanagedDataReader((BaseReader)this.ExecuteReader(), ManagedConnection);
        }

        protected abstract class MaybeManagedDataReader : IDataReader
        {
            public BaseReader WrappedReader { get; }
            public MaybeManagedConnection ManagedConnection { get; }

            public MaybeManagedDataReader(BaseReader rdr, MaybeManagedConnection conn)
            {
                WrappedReader = rdr;
                ManagedConnection = conn;
            }

            public object this[int i] => WrappedReader[i];

            public object this[string name] => WrappedReader[name];

            public int Depth => WrappedReader.Depth;

            public bool IsClosed => WrappedReader.IsClosed;

            public int RecordsAffected => WrappedReader.RecordsAffected;

            public int FieldCount => WrappedReader.FieldCount;

            public abstract void Close();

            public abstract void Dispose();

            public bool GetBoolean(int i) => WrappedReader.GetBoolean(i);

            public byte GetByte(int i) => WrappedReader.GetByte(i);

            public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => WrappedReader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);

            public char GetChar(int i) => WrappedReader.GetChar(i);

            public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => WrappedReader.GetChars(i, fieldoffset, buffer, bufferoffset, length);

            public IDataReader GetData(int i) => WrappedReader.GetData(i);

            public string GetDataTypeName(int i) => WrappedReader.GetDataTypeName(i);

            public DateTime GetDateTime(int i) => WrappedReader.GetDateTime(i);

            public decimal GetDecimal(int i) => WrappedReader.GetDecimal(i);

            public double GetDouble(int i) => WrappedReader.GetDouble(i);

            public Type GetFieldType(int i) => WrappedReader.GetFieldType(i);

            public float GetFloat(int i) => WrappedReader.GetFloat(i);

            public Guid GetGuid(int i) => WrappedReader.GetGuid(i);

            public short GetInt16(int i) => WrappedReader.GetInt16(i);

            public int GetInt32(int i) => WrappedReader.GetInt32(i);

            public long GetInt64(int i) => WrappedReader.GetInt64(i);

            public string GetName(int i) => WrappedReader.GetName(i);

            public int GetOrdinal(string name) => WrappedReader.GetOrdinal(name);

            public DataTable GetSchemaTable() => WrappedReader.GetSchemaTable();

            public string GetString(int i) => WrappedReader.GetString(i);

            public object GetValue(int i) => WrappedReader.GetValue(i);

            public int GetValues(object[] values) => WrappedReader.GetValues(values);

            public bool IsDBNull(int i) => WrappedReader.IsDBNull(i);

            public bool NextResult() => WrappedReader.NextResult();

            public bool Read() => WrappedReader.Read();
        }

        protected sealed class ManagedDataReader : MaybeManagedDataReader
        {
            public ManagedDataReader(BaseReader rdr, MaybeManagedConnection conn) : base(rdr, conn) { }

            public override void Close() { }

            public override void Dispose()
            {
                WrappedReader.Dispose();
                ManagedConnection.Dispose();
            }
        }

        protected sealed class UnmanagedDataReader : MaybeManagedDataReader
        {
            public UnmanagedDataReader(BaseReader rdr, MaybeManagedConnection conn) : base(rdr, conn) { }

            public override void Close() { }

            public override void Dispose() { }
        }

        protected abstract class MaybeManagedConnection : IDbConnection
        {
            public DbConnection WrappedConnection { get; }

            public static implicit operator DbConnection(MaybeManagedConnection mc) => mc.WrappedConnection;

            protected MaybeManagedConnection(DbConnection connection) => this.WrappedConnection = connection;

            public string ConnectionString { get => WrappedConnection.ConnectionString; set => WrappedConnection.ConnectionString = value; }

            public int ConnectionTimeout => WrappedConnection.ConnectionTimeout;

            public string Database => WrappedConnection.Database;

            public ConnectionState State => WrappedConnection.State;

            public IDbTransaction BeginTransaction() => WrappedConnection.BeginTransaction();

            public IDbTransaction BeginTransaction(IsolationLevel il) => WrappedConnection.BeginTransaction(il);

            public void ChangeDatabase(string databaseName) => WrappedConnection.ChangeDatabase(databaseName);

            public abstract void Close();

            public IDbCommand CreateCommand() => WrappedConnection.CreateCommand();

            public abstract MaybeManagedCommand CreatedManagedCommand();

            public abstract void Dispose();

            public abstract void Open();

            public struct OfSomeKind
            {
                readonly MaybeManagedConnection connection;

                public OfSomeKind(DbConnection conn, Func<DbConnection> create_connection)
                {
                    if (conn is null)
                        connection = new ManagedConnection(create_connection());
                    else
                        connection = new UnmanagedConnection(conn);
                }

                public static implicit operator MaybeManagedConnection(OfSomeKind k) => k.connection;
            }

            sealed class ManagedConnection : MaybeManagedConnection
            {
                public ManagedConnection(DbConnection conn) : base(conn) { }

                public override void Close() => WrappedConnection.Close();

                public override MaybeManagedCommand CreatedManagedCommand() => new ManagedCommand((DbCommand)CreateCommand(), this);

                public override void Dispose()
                {
                    #if DEBUG
                    Console.WriteLine("Connection Disposed!");
                    #endif

                    WrappedConnection.Dispose();
                }

                public override void Open()
                {
                    if (WrappedConnection.State == ConnectionState.Closed)
                        WrappedConnection.Open();
                }
            }

            sealed class UnmanagedConnection : MaybeManagedConnection
            {
                public UnmanagedConnection(DbConnection conn) : base(conn) { }

                public override void Close() { }

                public override MaybeManagedCommand CreatedManagedCommand() => new UnmanagedCommand((DbCommand)CreateCommand(), this);

                public override void Dispose() { }

                public override void Open() { }
            }
        }
    }

    public abstract partial class Proc<DbParameter, DbConnection, DbCommand, DbProxyFactory, BaseReader, EnhancedReader, Result>
        where DbParameter : IDataParameter
        where DbConnection : class, IDbConnection
        where DbCommand : class, IDbCommand
        where BaseReader :  IDataReader
        where EnhancedReader : IDataReader
        where DbProxyFactory : DbReaderProxyFactory<BaseReader, EnhancedReader>, new()     
    {
        readonly Func<string> sql;
        readonly List<DbParameter> parameters = new List<DbParameter>();

        public Proc(Func<string> sql)
        {
            this.sql = sql;
        }

        public Proc(string sql)
        {
            if (sql is null)
            {
                var type = GetType();
                var name = type.Name;
                var schemas = from attr in type.GetCustomAttributes(typeof(SchemaAttribute), false) select ((SchemaAttribute)attr).Name;

                var schema = schemas.Any() ? string.Join(".", schemas) + "." : null;

                this.sql = () => schema + type.Name;
            }
            else
                this.sql = () => sql;
        }

        protected void Add(DbParameter param) => parameters.Add(param);

        protected virtual string ConnectionString => ConfigurationManager.ConnectionStrings["default"].ConnectionString;
        protected abstract DbConnection CreateConnection();
        protected abstract CommandType GetCommandType(string s);

        protected abstract Result _Execute<K>(K cmd) where K : CommandEx<EnhancedReader>;

        protected abstract void AddParameter(DbCommand cmd, DbParameter param);

        protected struct Command : CommandEx<EnhancedReader>
        {
            private MaybeManagedCommand cmd;

            DbProxyFactory proxy_factory;

            public Command(MaybeManagedCommand cmd)
            {
                this.cmd = cmd;
                proxy_factory = new DbProxyFactory();                
            }

            public DbCommand WrappedCommand => cmd.WrappedCommand;

            public EnhancedReader ExecuteReader()
            {
                cmd.ManagedConnection.Open();

                return proxy_factory.CreateReader((BaseReader) cmd.ExecuteReader());
            }

            public struct Enumerator : IEnumerator<EnhancedReader>
            {
                readonly DbProxyFactory factory;
                readonly MaybeManagedDataReader reader;

                public Enumerator(MaybeManagedDataReader rdr, DbProxyFactory factory)
                {
                    rdr.ManagedConnection.Open();
                    reader = rdr;
                    this.factory = factory;
                }

                public EnhancedReader Current => factory.CreateReader(reader.WrappedReader);

                object IEnumerator.Current => reader;

                public void Reset() { }

                public bool MoveNext() => reader.Read();

                public void Dispose() => reader.Dispose();
            }

            IEnumerator<EnhancedReader> IEnumerable<EnhancedReader>.GetEnumerator()
            {
                return new Enumerator(cmd.GetManagedReader(), proxy_factory);
            }

            IEnumerator IEnumerable.GetEnumerator() => (this as IEnumerable<DbProxyFactory>).GetEnumerator();

            public int ExecuteNonQuery()
            {
                cmd.Connection.Open();

                return cmd.ExecuteNonQuery();
            }
        }

        public Result Execute() => Execute(null);

        public Result Execute(DbConnection conn)
        {
            var sql = this.sql().Trim();

            MaybeManagedConnection managedConnection = new MaybeManagedConnection.OfSomeKind(conn, CreateConnection);

            var cmd = managedConnection.CreatedManagedCommand();
            cmd.CommandText = sql;
            cmd.CommandType = GetCommandType(sql);

            foreach (var param in parameters)
                AddParameter(cmd.WrappedCommand, param);

            return _Execute(new Command(cmd));
        }
    }

    public sealed class StandardSqlReaderProxyFactory : SqlServerProxyFactory<StandardSqlReaderProxyFactory.Wrapped>
    {
        public override Wrapped CreateReader(SqlDataReader rdr) => new Wrapped(rdr);

        public struct Wrapped : IDataReader
        {
            readonly SqlDataReader rdr;

            public Wrapped(SqlDataReader rdr)
            {
                this.rdr = rdr;
            }

            public object this[int i] => rdr[i];

            public object this[string name] => rdr[name];

            public int Depth => rdr.Depth;

            public bool IsClosed => rdr.IsClosed;

            public int RecordsAffected => rdr.RecordsAffected;

            public int FieldCount => rdr.FieldCount;

            public void Close()
            {
                rdr.Close();
            }

            public void Dispose()
            {
                rdr.Dispose();
            }

            public bool GetBoolean(int i)
            {
                return rdr.GetBoolean(i);
            }

            public byte GetByte(int i)
            {
                return rdr.GetByte(i);
            }

            public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
            {
                return rdr.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
            }

            public char GetChar(int i)
            {
                return rdr.GetChar(i);
            }

            public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
            {
                return rdr.GetChars(i, fieldoffset, buffer, bufferoffset, length);
            }

            public IDataReader GetData(int i)
            {
                return ((IDataReader)rdr).GetData(i);
            }

            public string GetDataTypeName(int i)
            {
                return rdr.GetDataTypeName(i);
            }

            public DateTime GetDateTime(int i)
            {
                return rdr.GetDateTime(i);
            }

            public decimal GetDecimal(int i)
            {
                return rdr.GetDecimal(i);
            }

            public double GetDouble(int i)
            {
                return rdr.GetDouble(i);
            }

            public Type GetFieldType(int i)
            {
                return rdr.GetFieldType(i);
            }

            public float GetFloat(int i)
            {
                return rdr.GetFloat(i);
            }

            public Guid GetGuid(int i)
            {
                return rdr.GetGuid(i);
            }

            public short GetInt16(int i)
            {
                return rdr.GetInt16(i);
            }

            public int GetInt32(int i)
            {
                return rdr.GetInt32(i);
            }

            public long GetInt64(int i)
            {
                return rdr.GetInt64(i);
            }

            public string GetName(int i)
            {
                return rdr.GetName(i);
            }

            public int GetOrdinal(string name)
            {
                return rdr.GetOrdinal(name);
            }

            public DataTable GetSchemaTable()
            {
                return rdr.GetSchemaTable();
            }

            public string GetString(int i)
            {
                return rdr.GetString(i);
            }

            public object GetValue(int i)
            {
                return rdr.GetValue(i);
            }

            public int GetValues(object[] values)
            {
                return rdr.GetValues(values);
            }

            public bool IsDBNull(int i)
            {
                return rdr.IsDBNull(i);
            }

            public bool NextResult()
            {
                return rdr.NextResult();
            }

            public bool Read()
            {
                return rdr.Read();
            }
        }
    }

    public abstract class StandardReaderEnhancement<T> : IDataReader
        where T : IDataReader
    {
        readonly T rdr;

        protected StandardReaderEnhancement(T rdr) => this.rdr =  rdr;
        

        public object this[int i] => rdr[i];

        public object this[string name] => rdr[name];

        public int Depth => rdr.Depth;

        public bool IsClosed => rdr.IsClosed;

        public int RecordsAffected => rdr.RecordsAffected;

        public int FieldCount => rdr.FieldCount;

        public void Close()
        {
            rdr.Close();
        }

        public void Dispose()
        {
            rdr.Dispose();
        }

        public bool GetBoolean(int i)
        {
            return rdr.GetBoolean(i);
        }

        public byte GetByte(int i)
        {
            return rdr.GetByte(i);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return rdr.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        }

        public char GetChar(int i)
        {
            return rdr.GetChar(i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return rdr.GetChars(i, fieldoffset, buffer, bufferoffset, length);
        }

        public IDataReader GetData(int i)
        {
            return rdr.GetData(i);
        }

        public string GetDataTypeName(int i)
        {
            return rdr.GetDataTypeName(i);
        }

        public DateTime GetDateTime(int i)
        {
            return rdr.GetDateTime(i);
        }

        public decimal GetDecimal(int i)
        {
            return rdr.GetDecimal(i);
        }

        public double GetDouble(int i)
        {
            return rdr.GetDouble(i);
        }

        public Type GetFieldType(int i)
        {
            return rdr.GetFieldType(i);
        }

        public float GetFloat(int i)
        {
            return rdr.GetFloat(i);
        }

        public Guid GetGuid(int i)
        {
            return rdr.GetGuid(i);
        }

        public short GetInt16(int i)
        {
            return rdr.GetInt16(i);
        }

        public int GetInt32(int i)
        {
            return rdr.GetInt32(i);
        }

        public long GetInt64(int i)
        {
            return rdr.GetInt64(i);
        }

        public string GetName(int i)
        {
            return rdr.GetName(i);
        }

        public int GetOrdinal(string name)
        {
            return rdr.GetOrdinal(name);
        }

        public DataTable GetSchemaTable()
        {
            return rdr.GetSchemaTable();
        }

        public string GetString(int i)
        {
            return rdr.GetString(i);
        }

        public object GetValue(int i)
        {
            return rdr.GetValue(i);
        }

        public int GetValues(object[] values)
        {
            return rdr.GetValues(values);
        }

        public bool IsDBNull(int i)
        {
            return rdr.IsDBNull(i);
        }

        public bool NextResult()
        {
            return rdr.NextResult();
        }

        public bool Read()
        {
            return rdr.Read();
        }
    }

    public abstract class SqlServerProxyFactory<T> : DbReaderProxyFactory<SqlDataReader, T>
        where T : IDataReader
    { }

    public abstract class SqlProc<T> : SqlProc<T, StandardSqlReaderProxyFactory, StandardSqlReaderProxyFactory.Wrapped>
    {
        public SqlProc() { }
        public SqlProc(string sql) : base(sql) { }
        
    }

    public abstract class SqlProc<T, ProxyFactory, EnhancedReader> : Proc<SqlParameter, SqlConnection, SqlCommand, ProxyFactory, SqlDataReader, EnhancedReader, T>
        where EnhancedReader : IDataReader
        where ProxyFactory : SqlServerProxyFactory<EnhancedReader>, new()
    {
        public SqlProc() : base((string)null) { }
        public SqlProc(string sql) : base(sql) { }
        public SqlProc(Func<string> sql) : base(sql) { }

        protected override CommandType GetCommandType(string s) => s.StartsWith("[") || s.All(c => !char.IsWhiteSpace(c)) ? CommandType.StoredProcedure : CommandType.Text;

        protected override SqlConnection CreateConnection() => new SqlConnection(ConnectionString);

        protected override void AddParameter(SqlCommand cmd, SqlParameter param) => cmd.Parameters.Add(param);

        SqlParameter CreateParameter<V>(string name, V value)
        {
            var type = typeof(V);

            var param = new SqlParameter(name, value);

            if (type == typeof(int))
            {
                param.DbType = DbType.Int32;
                param.SqlDbType = SqlDbType.Int;
            }
            else

            if (type == typeof(short))
            {
                param.DbType = DbType.Int16;
                param.SqlDbType = SqlDbType.SmallInt;
            }
            else

            if (type == typeof(long))
            {
                param.DbType = DbType.Int64;
                param.SqlDbType = SqlDbType.BigInt;
            }
            else

            if (type == typeof(byte))
            {
                param.DbType = DbType.Byte;
                param.SqlDbType = SqlDbType.TinyInt;
            }
            else

            if (type == typeof(decimal))
            {
                param.DbType = DbType.Decimal;
                param.SqlDbType = SqlDbType.Decimal;
            }
            else

            if (type == typeof(double))
            {
                param.DbType = DbType.Double;
                param.SqlDbType = SqlDbType.Real;
            }
            else

            if (type == typeof(float))
            {
                param.DbType = DbType.Single;
                param.SqlDbType = SqlDbType.Float;
            }
            else

            if (type == typeof(Guid))
            {
                param.DbType = DbType.Guid;
                param.SqlDbType = SqlDbType.UniqueIdentifier;
            }
            else

            if (type == typeof(bool))
            {
                param.DbType = DbType.Boolean;
                param.SqlDbType = SqlDbType.Bit;
            }
            else

            if (type == typeof(DateTime))
            {
                param.DbType = DbType.DateTime;
                param.SqlDbType = SqlDbType.DateTime;
            }
            else

            if (type == typeof(string))
            {
                var s = (string)(object)value;

                if (s?.Length > 4000)
                {
                    param.DbType = DbType.String;
                    param.SqlDbType = SqlDbType.NText;
                }
                else
                {
                    param.SqlDbType = SqlDbType.NVarChar;
                    param.DbType = DbType.StringFixedLength;
                }
            }
            else
            {
                param.DbType = DbType.Object;
                param.SqlDbType = SqlDbType.Variant;
            }

            return param;
        }

        protected void Add<V>(string name, V value)
        {
            if (!name.StartsWith("@"))
                name = "@" + name;

            Add(CreateParameter(name, value));
        }

        protected void Add<V>(ValueTuple<string, V> x)
        {
            var (name, value) = x;

            Add(name, value);
        }

        protected void Add<V>(Tuple<string, V> xs) //For F# compatibility
        {
            Add(xs.Item1, xs.Item2);
        }
    }
}
