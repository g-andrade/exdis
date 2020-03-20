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
      {:unknown_command, name} ->
        "ERR unknown command '#{name}'"
      :key_of_wrong_type ->
        "WRONGTYPE Operation against a key holding the wrong kind of value"
      :value_not_an_integer_or_out_of_range ->
        "ERR value is not an integer or out of range"
      :increment_or_decrement_would_overflow ->
        "ERR increment or decrement would overflow"
      :value_not_a_valid_float ->
        "ERR value is not a valid float"
      :increment_would_produce_NaN_or_infinity ->
        "ERR increment would produce NaN or Infinity"
    end
  end
end
