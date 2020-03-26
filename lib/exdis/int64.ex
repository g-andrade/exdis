defmodule Exdis.Int64 do
  ## ------------------------------------------------------------------
  ## Constant Definitions
  ## ------------------------------------------------------------------

  @min -0x8000000000000000
  @max +0x7FFFFFFFFFFFFFFF
  @max_decimal_string_length 20

  ## ------------------------------------------------------------------
  ## Record and Type Definitions
  ## ------------------------------------------------------------------

  @opaque t :: -0x8000000000000000..0x7FFFFFFFFFFFFFFF

  ## ------------------------------------------------------------------
  ## API Functions
  ## ------------------------------------------------------------------

  def add(integer, increment) do
    case integer + increment do
      integer when integer >= @min and integer <= @max ->
        {:ok, integer}
      _ when integer >= @min and integer <= @max and increment > 0 ->
        {:error, :overflow_or_underflow}
      _ when integer >= @min and integer <= @max ->
        {:error, :underflow_or_underflow}
    end
  end

  def decimal_string_length(integer) do
    byte_size( to_decimal_string(integer) )
  end

  def from_decimal_string(string, expected_trailing_data \\ "") do
    case (
      byte_size(string) < (@max_decimal_string_length + byte_size(expected_trailing_data))
      and Integer.parse(string))
    do
      {integer, ^expected_trailing_data} when integer >= @min and integer <= @max ->
        {:ok, integer}
      {_integer, <<unexpected_trailing_data :: bytes>>} ->
        {:error, {:unexpected_trailing_data, unexpected_trailing_data}}
      :error ->
        {:error, {:not_an_integer, string}}
      false ->
        {:error, {:string_too_large, byte_size(string)}}
    end
  end

  def from_float(float) do
    case trunc(float) do
      integer when integer == float ->
        {:ok, integer}
      _ ->
        {:error, :loss_of_precision}
    end
  end

  def min(), do: @min
  def max(), do: @max
  def max_decimal_string_length(), do: @max_decimal_string_length

  def new(integer) when is_integer(integer) and integer >= @min and integer <= @max do
    integer
  end

  def to_decimal_string(integer) do
    Integer.to_string(integer)
  end
end
