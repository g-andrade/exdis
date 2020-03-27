defmodule Exdis.Command do
  require Logger

  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def recv(recv_fun) do
    #Logger.info("Receiving command")
    case Exdis.RESP.Value.recv(recv_fun) do
      {:array, [{:string, name} | args]} ->
        #Logger.info("Command received: #{inspect {name, args}}")
        name = String.upcase(name)
        parse(name, args)
    end
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp parse(name, args) do
    parsers = known_parsers()
    parser = Map.get(parsers, name, &parse_unknown_command(name, &1))
    case parser.(args) do
      {:error, reason} ->
        {:error, {:parse, reason}}
      result ->
        result
    end
  end

  defp known_parsers() do
    %{
      # Keys
      "DEL" => &Exdis.CommandParsers.Key.delete/1,
      "EXISTS" => &Exdis.CommandParsers.Key.exist?/1,
      "KEYS" => &Exdis.CommandParsers.Key.keys/1,

      # Server
      "FLUSHDB" => &Exdis.CommandParsers.Server.flush_db/1,

      # Strings
      "APPEND" => &Exdis.CommandParsers.String.append/1,
      "BITCOUNT" => &Exdis.CommandParsers.String.bit_count/1,
      "BITPOS" => &Exdis.CommandParsers.String.bit_position/1,
      "DECR" => &Exdis.CommandParsers.String.decrement/1,
      "DECRBY" => &Exdis.CommandParsers.String.decrement_by/1,
      "GET" => &Exdis.CommandParsers.String.get/1,
      "GETBIT" => &Exdis.CommandParsers.String.get_bit/1,
      "GETRANGE" => &Exdis.CommandParsers.String.get_range/1,
      "GETSET" => &Exdis.CommandParsers.String.get_set/1,
      "INCR" => &Exdis.CommandParsers.String.increment/1,
      "INCRBY" => &Exdis.CommandParsers.String.increment_by/1,
      "INCRBYFLOAT" => &Exdis.CommandParsers.String.increment_by_float/1,
      "MGET" => &Exdis.CommandParsers.String.mget/1,
      "MSET" => &Exdis.CommandParsers.String.mset/1,
      "SET" => &Exdis.CommandParsers.String.set/1,
      "STRLEN" => &Exdis.CommandParsers.String.str_length/1,

      # Transactions
      "DISCARD" => &Exdis.CommandParsers.Transaction.discard/1,
      "EXEC" => &Exdis.CommandParsers.Transaction.exec/1,
      "MULTI" => &Exdis.CommandParsers.Transaction.multi/1
    }
  end

  defp parse_unknown_command(name, _args) do
    {:error, {:unknown_command, name}}
  end
end
