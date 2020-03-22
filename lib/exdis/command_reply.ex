defmodule Exdis.CommandReply do
  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def encode(:ok) do
    Exdis.RESP.Value.encode({:simple_string, "OK"})
  end

  def encode({:error, reason}) do
    iodata = error_reason_to_iodata(reason)
    Exdis.RESP.Value.encode({:error, iodata})
  end

  def encode(other) do
    Exdis.RESP.Value.encode(other)
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp error_reason_to_iodata(reason) do
    case reason do
      :bad_syntax ->
        "ERR syntax error"
      {:unknown_command, command_name} ->
        "ERR unknown command '#{command_name}'"
      :key_of_wrong_type ->
        "WRONGTYPE Operation against a key holding the wrong kind of value"
      {:not_an_integer_or_out_of_range, argument_name} ->
        "ERR #{argument_name} is not an integer or out of range"
      :increment_or_decrement_would_overflow ->
        "ERR increment or decrement would overflow"
      {:not_a_valid_float, argument_name} ->
        "ERR #{argument_name} is not a valid float"
      :increment_would_produce_NaN_or_infinity ->
        "ERR increment would produce NaN or Infinity"
    end
  end
end
