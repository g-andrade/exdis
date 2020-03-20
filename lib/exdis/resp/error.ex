defmodule Exdis.RESP.Error do
  require Record

  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def parser() do
    Exdis.RESP.SimpleString.parser()
  end

  def encode(iodata) do
    Exdis.RESP.SimpleString.encode(iodata)
  end
end
