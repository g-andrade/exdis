defmodule Exdis.RESP.Line do
  use Bitwise
  require Record

  ## ------------------------------------------------------------------
  ## Macro-like Attribute Definitions
  ## ------------------------------------------------------------------

  @default_max_length (16 * (1 <<< 10))

  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def parser(opts \\ []) do
    max_length = Keyword.get(opts, :max_length, @default_max_length)
    line_acc = <<>>
    line_acc_size = 0
    &parse_line(&1, max_length, line_acc, line_acc_size)
  end

  def encode(string) do
    [string, ?\r, ?\n]
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp parse_line(data, max_length, line_acc, line_acc_size) do
    case :binary.match(data, <<?\r>>) do
      {index, 1} ->
        case data do
          <<line_suffix :: bytes-size(index), ?\r, ?\n, rest :: bytes>> ->
            line = :erlang.iolist_to_binary([line_acc, line_suffix])
            {:parsed, line, rest}
          <<possible_line_suffix :: bytes-size(index), ?\r>> ->
            # Only missing `\n`, but we can't be sure - another char may come after.
            # Still, we consume as much data as we can to optimize later parsing.
            line_acc = [line_acc, possible_line_suffix]
            line_acc_size = line_acc_size + index
            rest = <<?\r>>
            {:more, &parse_line(&1, max_length, line_acc, line_acc_size), rest}
          <<_ :: bytes>> ->
            # definitely not the end
            line_acc = [line_acc, data]
            line_acc_size = line_acc_size + byte_size(data)
            {:more, &parse_line(&1, max_length, line_acc, line_acc_size), <<>>}
        end
      :nomatch ->
        line_acc = [line_acc, data]
        line_acc_size = line_acc_size + byte_size(data)
        {:more, &parse_line(&1, max_length, line_acc, line_acc_size), <<>>}
    end
  end
end
