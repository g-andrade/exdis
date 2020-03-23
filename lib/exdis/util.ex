defmodule Exdis.RESP.Util do
  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  def maybe_copy_line_subbinary(subbinary, size) when size < 40 do
    :binary.copy(subbinary)
  end

  def maybe_copy_line_subbinary(subbinary, _size) do
    subbinary
  end
end
