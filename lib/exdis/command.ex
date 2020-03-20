defmodule Exdis.Command do
  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def parser() do
    Exdis.RESP.Value.parser()
  end

  def handle({:array, [{:string, name} | args]}, reply_cb) do
    upcase_name = String.upcase(name)
    known_command_handlers = known_command_handlers()
    handler = Map.get(known_command_handlers, upcase_name, &handle_unknown_command(name, &1, &2))
    handler.(args, reply_cb)
  end

  def handle(_, reply_cb) do
    reply_cb.(:sync, {:error, :bad_syntax})
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
      "INCR" => &Exdis.Commands.String.increment/2,
      "INCRBY" => &Exdis.Commands.String.increment_by/2,
      "INCRBYFLOAT" => &Exdis.Commands.String.increment_by_float/2,
      "SET" => &Exdis.Commands.String.set/2
    }
  end

  defp handle_unknown_command(name, _args, reply_cb) do
    reply_cb.(:sync, {:error, {:unknown_command, name}})
  end
end
