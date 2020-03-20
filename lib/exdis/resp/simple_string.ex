defmodule Exdis.RESP.SimpleString do
  use Bitwise

  ## ------------------------------------------------------------------
  ## Macro-like Attribute Definitions
  ## ------------------------------------------------------------------

  @default_max_length (512 * (1 <<< 20))

  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def parser(opts \\ []) do
    max_length = Keyword.get(opts, :max_length, @default_max_length)
    line_parser_opts = [max_length: max_length]
    Exdis.RESP.Line.parser(line_parser_opts)
  end

  def encode(iodata) do
    string = :erlang.iolist_to_binary(iodata)
    safe_string = :binary.replace(string, "\r\n", "\\r\\n", [:global])
    Exdis.RESP.Line.encode(safe_string)
  end
end
