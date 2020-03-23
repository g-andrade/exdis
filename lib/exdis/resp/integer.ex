defmodule Exdis.RESP.Integer do
  ## ------------------------------------------------------------------
  ## Public Functions
  ## ------------------------------------------------------------------

  def recv(fun) do
    line = fun.(:line)
    {:ok, integer} = Exdis.Int64.from_decimal_string(line, "\r\n")
    integer
  end
 
  def encode(value) do
    string = Integer.to_string(value)
    [string, "\r\n"]
  end
end
