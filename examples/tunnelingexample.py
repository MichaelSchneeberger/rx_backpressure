import rxbp

from rxbp import op


def add_output1_to_dict():
    return op.flat_map(lambda fdict: fdict['input'].pipe(
        op.filter(lambda v: v%2 == 0),
        op.share(lambda output1: rxbp.return_value({**fdict, 'output1': output1})),
    ))


def add_output2_to_dict():
    return op.flat_map(lambda fdict: fdict['input'].pipe(
        op.map(lambda v: v+100),
        op.share(lambda output2: rxbp.return_value({**fdict, 'output2': output2})),
    ))


result = rxbp.range(10).pipe(
    op.share(lambda input: rxbp.return_value({'input': input})),
    add_output1_to_dict(),
    add_output2_to_dict(),
    op.flat_map(lambda fdict: fdict['output1'].pipe(
        op.to_list(),
        op.zip(fdict['output2'].pipe(
            op.to_list(),
        ))
    ))
).run()

print(result)