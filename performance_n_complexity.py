def two_sums(nums, target):
    dicy = {}

    for i, num in enumerate(nums):
        if target - num in dicy:
            return [dicy[target - num], i]
        dicy[num] = i

print(two_sums([3, 3, 3], 6))
#output: [0, 1]
