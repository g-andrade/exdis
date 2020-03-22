defmodule Exdis.Command do
  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def parser() do
    Exdis.RESP.Value.parser()
  end

  def handle({:array, [{:string, name} | args]}) do
    upcase_name = String.upcase(name)
    known_command_handlers = known_command_handlers()
    handler = Map.get(known_command_handlers, upcase_name, &handle_unknown_command(name, &1))
    handler.(args)
  end

  def handle(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp known_command_handlers() do
    %{
      "APPEND" => &Exdis.Commands.String.append/1,
      "DECR" => &Exdis.Commands.String.decrement/1,
      "DECRBY" => &Exdis.Commands.String.decrement_by/1,
      "GET" => &Exdis.Commands.String.get/1,
      "GETBIT" => &Exdis.Commands.String.get_bit/1,
      "GETRANGE" => &Exdis.Commands.String.get_range/1,
      "GETSET" => &Exdis.Commands.String.get_set/1,
      "INCR" => &Exdis.Commands.String.increment/1,
      "INCRBY" => &Exdis.Commands.String.increment_by/1,
      "INCRBYFLOAT" => &Exdis.Commands.String.increment_by_float/1,
      "SET" => &Exdis.Commands.String.set/1,
      "STRLEN" => &Exdis.Commands.String.str_length/1
    }
  end

  defp handle_unknown_command(name, _args) do
    {:error, {:unknown_command, name}}
  end
end
