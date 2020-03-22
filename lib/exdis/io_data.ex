defmodule Exdis.IoData do
  use Bitwise
  require Record

  ## ------------------------------------------------------------------
  ## Record and Type Definitions
  ## ------------------------------------------------------------------

  Record.defrecord(:io_data,
    bytes: nil,
    size: nil,
    fragments: nil
  )

  @opaque t :: record(:io_data,
    bytes: iodata,
    size: non_neg_integer,
    fragments: non_neg_integer)

  ## ------------------------------------------------------------------
  ## API Functions
  ## ------------------------------------------------------------------

  def append(io_data(bytes: bytes, size: size, fragments: fragments) = io_data, tail_bytes) do
    {tail_size, tail_fragments} = count_size_and_fragments(tail_bytes)
    io_data(io_data,
      bytes: [bytes, tail_bytes],
      size: size + tail_size,
      fragments: fragments + tail_fragments)
  end

  def bytes(io_data(bytes: bytes)), do: bytes

  def flatten(io_data(bytes: bytes, size: size) = io_data) do
    binary = :erlang.iolist_to_binary(bytes)
    ^size = byte_size(binary)
    io_data(io_data, bytes: binary, size: size, fragments: 1)
  end

  def fragments(io_data(fragments: fragments)), do: fragments

  # optimization
  def get_bit(io_data(size: size), offset) when offset >= size * 8 do
    0
  end

  def get_bit(io_data(bytes: bytes), offset) when offset >= 0 do
    case find_bit_recur(bytes, offset) do
      {:found, bit_value} ->
        bit_value
      {:skipped, _} ->
        0
    end
  end

  def get_range(io_data(bytes: bytes, size: size), start, finish) do
    start = max(0, normalize_byte_offset(start, size))
    finish = min(size - 1, normalize_byte_offset(finish, size))

    case start >= 0 and start < size and start <= finish do
      true  ->
        length = finish - start + 1
        {_, 0, range_bytes} = get_range_recur(bytes, start, length, [])
        range_bytes
      false ->
        ""
    end
  end

  def new(bytes) do
    {size, fragments} = count_size_and_fragments(bytes)
    io_data(
      bytes: bytes,
      size: size,
      fragments: fragments)
  end

  def size(io_data(size: size)), do: size

  ## ------------------------------------------------------------------
  ## Private Function: count_size_and_fragments
  ## ------------------------------------------------------------------

  defp count_size_and_fragments(bytes) do
    count_size_and_fragments_recur(bytes, 0, 0)
  end

  defp count_size_and_fragments_recur(<<binary :: bytes>>, size_acc, fragments_acc) do
    {size_acc + byte_size(binary), fragments_acc + 1}
  end

  defp count_size_and_fragments_recur([list_head | list_tail], size_acc, fragments_acc) do
    {size_acc, fragments_acc} = count_size_and_fragments_recur(list_head, size_acc, fragments_acc)
    count_size_and_fragments_recur(list_tail, size_acc, fragments_acc)
  end

  defp count_size_and_fragments_recur([], size_acc, fragments_acc) do
    {size_acc, fragments_acc}
  end

  defp count_size_and_fragments_recur(byte, size_acc, fragments_acc)
  when is_integer(byte) and byte >= 0 and byte < 256
  do
    {size_acc + 1, fragments_acc + 1}
  end

  ## ------------------------------------------------------------------
  ## Private Functions: count_size_and_fragments
  ## ------------------------------------------------------------------

  defp get_range_recur(_bytes, start, length, chunks_acc) when length === 0 do
    range_bytes = Enum.reverse(chunks_acc)
    {start, length, range_bytes}
  end

  defp get_range_recur(<<binary :: bytes>>, start, length, chunks_acc) do
    binary_size = byte_size(binary)
    case start >= binary_size do
      true ->
        start = start - binary_size
        {start, length, chunks_acc}
      false ->
        chunk_size = min(length, binary_size - start)
        <<_ :: bytes-size(start), chunk :: bytes-size(chunk_size), _ :: bytes>> = binary
        start = 0
        length = length - chunk_size
        chunks_acc = [chunk | chunks_acc]
        {start, length, chunks_acc}
    end
  end

  defp get_range_recur([list_head | list_tail], start, length, chunks_acc) do
    {start, length, chunks_acc} = get_range_recur(list_head, start, length, chunks_acc)
    get_range_recur(list_tail, start, length, chunks_acc)
  end

  defp get_range_recur([], start, length, chunks_acc) do
    {start, length, chunks_acc}
  end

  defp get_range_recur(byte, start, length, chunks_acc) when is_integer(byte) do
    start = start + 1
    length = length - 1
    chunks_acc = [byte | chunks_acc]
    {start, length, chunks_acc}
  end

  ## ------------------------------------------------------------------
  ## Private Function: find_bit_recur
  ## ------------------------------------------------------------------

  defp find_bit_recur(<<binary :: bytes>>, offset) do
    case binary do
      <<_ :: bits-size(offset), bit_value :: 1, _ :: bits>> ->
        # bit value found within binary
        {:found, bit_value}
      _ ->
        {:skipped, offset - bit_size(binary)}
    end
  end

  defp find_bit_recur([list_head | list_tail], offset) do
    case find_bit_recur(list_head, offset) do
      {:found, _} = found ->
        found
      {:skipped, new_offset} ->
        find_bit_recur(list_tail, new_offset)
    end
  end

  defp find_bit_recur([], offset) do
    {:skipped, offset}
  end

  defp find_bit_recur(byte, offset) when is_integer(byte) do
    case offset < 8 do
      true ->
        bit_value = (byte >>> offset) &&& 1
        {:found, bit_value}
      false ->
        {:skipped, 8}
    end
  end

  ## ------------------------------------------------------------------
  ## Private Functions: normalize_byte_offset
  ## ------------------------------------------------------------------

  defp normalize_byte_offset(offset, _size) when offset >= 0, do: offset
  defp normalize_byte_offset(offset, size), do: size + offset
end
