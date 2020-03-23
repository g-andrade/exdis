defmodule Exdis.Command do
  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def recv(fun) do
    case Exdis.RESP.Value.recv(fun) do
      {:array, [{:string, name} | args]} ->
        name = String.upcase(name)
        {name, args}
    end
  end

  def handle(database, name, args) do
    known_command_handlers = known_command_handlers()
    handler = Map.get(known_command_handlers, name, &handle_unknown_command(&1, name, &2))
    handler.(database, args)
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp known_command_handlers() do
    %{
      # Keys
      "KEYS" => &Exdis.Commands.Keys.keys/2,

      # Server
      "FLUSHDB" => &Exdis.Commands.Server.flush_db/2,

      # String commands
      "APPEND" => &Exdis.Commands.String.append/2,
      "DECR" => &Exdis.Commands.String.decrement/2,
      "DECRBY" => &Exdis.Commands.String.decrement_by/2,
      "GET" => &Exdis.Commands.String.get/2,
      "GETBIT" => &Exdis.Commands.String.get_bit/2,
      "GETRANGE" => &Exdis.Commands.String.get_range/2,
      "GETSET" => &Exdis.Commands.String.get_set/2,
      "INCR" => &Exdis.Commands.String.increment/2,
      "INCRBY" => &Exdis.Commands.String.increment_by/2,
      "INCRBYFLOAT" => &Exdis.Commands.String.increment_by_float/2,
      "SET" => &Exdis.Commands.String.set/2,
      "STRLEN" => &Exdis.Commands.String.str_length/2
    }
  end

  defp handle_unknown_command(_database, name, _args) do
    {:error, {:unknown_command, name}}
  end
end
