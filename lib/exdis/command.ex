defmodule Exdis.Command do
  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def parser() do
    Exdis.RESP.Value.parser()
  end

  def handle(database, {:array, [{:string, name} | args]}) do
    upcase_name = String.upcase(name)
    known_command_handlers = known_command_handlers()
    handler = Map.get(known_command_handlers, upcase_name, &handle_unknown_command(&1, name, &2))
    handler.(database, args)
  end

  def handle(_database, _resp_value) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp known_command_handlers() do
    %{
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
