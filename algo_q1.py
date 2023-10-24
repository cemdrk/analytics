def longest_substring(s: str) -> str:
    """Finds longest substring without repeating characters,
    O(n) time and O(n) space complexity
    which n is the length of s.
    :param str s: Source string.
    :returns: Longest substring.
    :rtype: str
    """
    if len(s) == 0:
        return 0

    last_seen = {}
    max_len = max_start = start = 0

    for i, chr in enumerate(s):  # O(n) time
        last_seen_index = last_seen.get(chr)  # O(1)
        if last_seen_index is not None and start <= last_seen_index:
            start = last_seen_index + 1
        else:
            if (i - start + 1) > max_len:
                max_len = i - start + 1
                max_start = start
        last_seen[chr] = i  # O(n) space

    longest_sub = s[max_start : max_start + max_len]
    return longest_sub 


def main():
    prompt = 'input: '
    user_input = input(prompt)
    longest_sub = longest_substring(user_input)

    output = f'output: {longest_sub} length: {len(longest_sub)}'
    print(output)


if __name__ == "__main__":
    main()
